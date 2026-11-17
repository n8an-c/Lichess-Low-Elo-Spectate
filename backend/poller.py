"""Status poller with hot/cold tiers: finds pooled players who just started a game.

Uses ``GET /api/users/status?withGameMetas=true`` (100 IDs per request),
which reports ``online`` and ``playing: {"id": ..., "clock": "3+0",
"variant"?: ...}`` for each user.

Only a few percent of a pool is online at any moment, so polling everyone at
the same rate wastes almost every request and makes the notice delay grow
with the pool size. Instead:

- the *hot* set (players reported online at their last sighting) is polled
  every ``hot_poll_seconds`` in one or two requests;
- the *cold* set (everyone else) is swept a few 100-ID batches per tick, and
  a player reported online joins the hot set immediately.

A "pass" is one complete sweep of the cold set.
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
    def __init__(
        self,
        client: LichessClient,
        db: Database,
        settings: DetectionSettings,
        queue: asyncio.Queue,
        pending: set[str] | None = None,
    ):
        self.client = client
        self.db = db
        self.settings = settings
        self.queue = queue
        # Game IDs queued or being verified; shared with the games-by-users
        # watcher so the same game is never queued twice.
        self._pending: set[str] = pending if pending is not None else set()
        self.clocks = set(settings.time_controls)
        self.hot: dict[str, float] = {}  # player id -> monotonic time last reported online
        self.ticks = 0
        self.passes = 0  # completed cold sweeps
        self.requests = 0
        self.total_queued = 0
        self.last_sweep_seconds = 0.0
        self.pool_size = 0
        self._sweep_started = time.monotonic()
        self._sweep_playing: set[str] = set()
        self._sweep_matching: set[str] = set()
        self._sweep_queued = 0
        self._cold_cursor = 0
        self._last_summary = 0.0
        # Kept for log/stat compatibility: counts from the last completed sweep.
        self.last_pass_online = 0
        self.last_pass_playing = 0
        self.last_pass_matching = 0

    # -- status handling ---------------------------------------------------------------

    def detect(self, statuses: list[dict], now: float | None = None) -> list[str]:
        """Update the hot set from a status response and return new game IDs worth verifying."""
        now = time.monotonic() if now is None else now
        new: list[str] = []
        for entry in statuses:
            pid = entry.get("id")
            if pid:
                if entry.get("online"):
                    self.hot[pid] = now
                else:
                    self.hot.pop(pid, None)
            playing = entry.get("playing")
            if not isinstance(playing, dict):
                continue
            game_id = playing.get("id")
            if not game_id:
                continue
            if pid:
                self._sweep_playing.add(pid)
            if playing.get("clock") not in self.clocks:
                continue
            variant = playing.get("variant")
            if variant not in (None, "standard"):
                continue
            if pid:
                self._sweep_matching.add(pid)
            if game_id in self._pending or self.db.game_seen(game_id):
                continue
            self._pending.add(game_id)
            new.append(game_id)
        return new

    def release(self, game_id: str) -> None:
        """Called by the verifier once a game has been checked and recorded."""
        self._pending.discard(game_id)

    async def _poll(self, batch: list[str]) -> None:
        try:
            statuses = await self.client.users_status(batch, with_game_metas=True)
        except LichessError as exc:
            log.warning("status poll failed (%s); continuing", exc)
            return
        except Exception:
            log.exception("status poll crashed; continuing with the next batch")
            return
        self.requests += 1
        for game_id in self.detect(statuses):
            log.info("found %s game %s (https://lichess.org/%s); verifying", self.settings.label, game_id, game_id)
            await self.queue.put(game_id)
            self._sweep_queued += 1
            self.total_queued += 1

    def _end_sweep(self) -> None:
        now = time.monotonic()
        self.passes += 1
        self.last_sweep_seconds = now - self._sweep_started
        self.last_pass_online = len(self.hot)
        self.last_pass_playing = len(self._sweep_playing)
        self.last_pass_matching = len(self._sweep_matching)
        msg = (
            f"poll pass {self.passes}: {self.pool_size} players, {len(self.hot)} online (hot), "
            f"{len(self._sweep_playing)} playing, {len(self._sweep_matching)} in {self.settings.label} games, "
            f"{self._sweep_queued} queued, sweep {self.last_sweep_seconds:.0f}s"
        )
        if now - self._last_summary >= 60:
            log.info(msg)
            self._last_summary = now
        else:
            log.debug(msg)
        self._sweep_started = now
        self._sweep_playing.clear()
        self._sweep_matching.clear()
        self._sweep_queued = 0

    # -- main loop -----------------------------------------------------------------------

    async def run(self) -> None:
        det = self.settings
        while True:
            tick_started = time.monotonic()
            ids = self.db.active_player_ids()
            self.pool_size = len(ids)
            if not ids:
                log.warning("player pool is empty; nothing to poll (build the pool first)")
                await asyncio.sleep(30)
                continue
            id_set = set(ids)
            for pid in [p for p in self.hot if p not in id_set]:
                del self.hot[pid]

            # 1. Everyone seen online recently: one request per 100.
            hot_ids = sorted(self.hot)
            for i in range(0, len(hot_ids), STATUS_BATCH):
                await self._poll(hot_ids[i : i + STATUS_BATCH])

            # 2. A few batches of the cold set, round-robin.
            cold_ids = [pid for pid in ids if pid not in self.hot]
            for _ in range(det.cold_batches_per_tick):
                if not cold_ids:
                    break
                if self._cold_cursor >= len(cold_ids):
                    self._cold_cursor = 0
                batch = cold_ids[self._cold_cursor : self._cold_cursor + STATUS_BATCH]
                self._cold_cursor += STATUS_BATCH
                await self._poll(batch)
                if self._cold_cursor >= len(cold_ids):
                    self._cold_cursor = 0
                    self._end_sweep()
                    break  # start the next sweep on the next tick

            self.ticks += 1
            elapsed = time.monotonic() - tick_started
            await asyncio.sleep(max(0.0, max(det.hot_poll_seconds, det.poll_interval_seconds) - elapsed))
