"""Status poller: finds pooled players who just started a game.

Uses ``GET /api/users/status?withGameMetas=true`` (100 IDs per request),
which reports ``playing: {"id": ..., "clock": "3+0", "variant"?: ...}`` for
each user currently in a game. Only games with the configured time control
and standard variant that have not been seen before are queued for
verification.
"""

from __future__ import annotations

import asyncio
import logging
import time

from .config import DetectionSettings
from .db import Database
from .lichess import LichessClient, LichessError

log = logging.getLogger(__name__)

STATUS_BATCH = 100


class Poller:
    def __init__(self, client: LichessClient, db: Database, settings: DetectionSettings, queue: asyncio.Queue):
        self.client = client
        self.db = db
        self.settings = settings
        self.queue = queue
        self._pending: set[str] = set()  # game IDs queued or being verified
        self.passes = 0
        self.last_pass_online = 0
        self.last_pass_playing = 0
        self.last_pass_matching = 0
        self.total_queued = 0

    def detect(self, statuses: list[dict]) -> list[str]:
        """Return the new game IDs worth verifying, and mark them pending."""
        new: list[str] = []
        for entry in statuses:
            if entry.get("online"):
                self.last_pass_online += 1
            playing = entry.get("playing")
            if not isinstance(playing, dict):
                continue
            self.last_pass_playing += 1
            game_id = playing.get("id")
            if not game_id:
                continue
            if playing.get("clock") != self.settings.time_control:
                continue
            variant = playing.get("variant")
            if variant not in (None, "standard"):
                continue
            self.last_pass_matching += 1
            if game_id in self._pending or self.db.game_seen(game_id):
                continue
            self._pending.add(game_id)
            new.append(game_id)
        return new

    def release(self, game_id: str) -> None:
        """Called by the verifier once a game has been checked and recorded."""
        self._pending.discard(game_id)

    async def run(self) -> None:
        last_summary = 0.0
        while True:
            ids = self.db.active_player_ids()
            if not ids:
                log.warning("player pool is empty; nothing to poll (build the pool first)")
                await asyncio.sleep(30)
                continue
            self.last_pass_online = self.last_pass_playing = self.last_pass_matching = 0
            queued = 0
            for i in range(0, len(ids), STATUS_BATCH):
                batch = ids[i : i + STATUS_BATCH]
                try:
                    statuses = await self.client.users_status(batch, with_game_metas=True)
                except LichessError as exc:
                    log.warning("status poll failed (%s); continuing", exc)
                    await asyncio.sleep(self.settings.poll_interval_seconds)
                    continue
                except Exception:
                    log.exception("status poll crashed; continuing with the next batch")
                    await asyncio.sleep(self.settings.poll_interval_seconds)
                    continue
                for game_id in self.detect(statuses):
                    log.info("found %s game %s (https://lichess.org/%s); verifying", self.settings.time_control, game_id, game_id)
                    await self.queue.put(game_id)
                    queued += 1
                await asyncio.sleep(self.settings.poll_interval_seconds)
            self.passes += 1
            self.total_queued += queued
            msg = (
                f"poll pass {self.passes}: {len(ids)} players, {self.last_pass_online} online, "
                f"{self.last_pass_playing} playing, "
                f"{self.last_pass_matching} in {self.settings.time_control} games, {queued} queued"
            )
            if time.monotonic() - last_summary >= 60:
                log.info(msg)
                last_summary = time.monotonic()
            else:
                log.debug(msg)
