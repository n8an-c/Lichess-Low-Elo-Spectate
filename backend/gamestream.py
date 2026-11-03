"""Push-based detection: long-lived ``POST /api/stream/games-by-users`` streams.

One stream per 300 active pool players. Lichess emits a game document when a
game between two listed players starts or finishes (and, with
``withCurrentGames``, the games already running when the stream opens).
Note the catch: a game is only reported when **both** players are in the
same list, so this source complements status polling rather than replacing
it; it gets more useful as opponent discovery grows the pool into a cluster
of players who meet each other.
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import aclosing

from .config import Settings
from .db import Database
from .lichess import LichessClient, LichessError
from .verifier import DiscoverCallback

log = logging.getLogger(__name__)

STREAM_BATCH = 300
LIVE_STATUSES = {"created", "started"}


def parse_time_control(time_control: str) -> tuple[int, int] | None:
    """'3+0' -> (180, 0); None if the string is not minutes+increment."""
    try:
        minutes, inc = time_control.split("+")
        return int(float(minutes) * 60), int(inc)
    except ValueError:
        return None


class GameStreamWatcher:
    def __init__(
        self,
        client: LichessClient,
        db: Database,
        settings: Settings,
        queue: asyncio.Queue,
        pending: set[str],
        pool_refreshed: asyncio.Event,
        discover: DiscoverCallback | None = None,
    ):
        self.client = client
        self.db = db
        self.settings = settings
        self.queue = queue
        self.pending = pending
        self.pool_refreshed = pool_refreshed
        self.discover = discover
        self.connections = 0
        self.docs = 0
        self.queued = 0
        self.streams = 0

    # -- document handling ---------------------------------------------------------

    def handle(self, doc: dict) -> str | None:
        """Queue the game for verification if it looks right; return its id when queued."""
        game_id = doc.get("id")
        if not game_id:
            return None
        reason = self._skip_reason(doc)
        if reason is not None:
            if doc.get("statusName") in LIVE_STATUSES:
                log.info("stream: game %s skipped (%s)", game_id, reason)
            return None
        self.pending.add(game_id)
        self.queue.put_nowait(game_id)
        self.queued += 1
        log.info(
            "stream: found %s game %s (https://lichess.org/%s); verifying",
            self.settings.detection.time_control, game_id, game_id,
        )
        return game_id

    def _skip_reason(self, doc: dict) -> str | None:
        """Why a games-by-users document should not be queued, or None if it should."""
        if doc.get("statusName") not in LIVE_STATUSES:
            return f"status {doc.get('statusName')}"  # finish events and aborted games
        if not doc.get("rated"):
            return "not rated"
        if doc.get("variant") != "standard":
            return f"variant {doc.get('variant')}"
        clock = doc.get("clock") or {}
        expected = parse_time_control(self.settings.detection.time_control)
        got = (clock.get("initial"), clock.get("increment"))
        if expected is not None and got != expected:
            return f"clock {got[0]}+{got[1]}"
        players = doc.get("players") or {}
        sides = [players.get("white") or {}, players.get("black") or {}]
        # Both players of a rated standard 3+0 game are worth knowing about,
        # even when the game itself is rejected on rating.
        if self.discover is not None:
            for side in sides:
                uid, rating = side.get("userId"), side.get("rating")
                if uid and isinstance(rating, int):
                    try:
                        self.discover(uid, rating)
                    except Exception:
                        log.exception("discover callback failed for %s", uid)
        cap = self.settings.pool.rating_cap
        for side in sides:
            rating = side.get("rating")
            if not isinstance(rating, int):
                return "anonymous or computer player"
            if rating >= cap:
                return f"{side.get('userId')} rated {rating} >= {cap}"
        if doc["id"] in self.pending:
            return "already queued"
        if self.db.game_seen(doc["id"]):
            return "already verified"
        return None

    # -- streams ---------------------------------------------------------------------

    async def run(self) -> None:
        while True:
            ids = self.db.active_player_ids()
            if not ids:
                log.warning("games-by-users: player pool is empty; nothing to stream")
                self.pool_refreshed.clear()
                try:
                    await asyncio.wait_for(self.pool_refreshed.wait(), timeout=30)
                except asyncio.TimeoutError:
                    pass
                continue
            batches = [ids[i : i + STREAM_BATCH] for i in range(0, len(ids), STREAM_BATCH)]
            self.pool_refreshed.clear()
            tasks = [
                asyncio.create_task(self._stream_batch(n, batch), name=f"games-by-users-{n}")
                for n, batch in enumerate(batches, 1)
            ]
            self.streams = len(tasks)
            log.info("games-by-users: %d stream(s) covering %d pooled players", len(tasks), len(ids))
            try:
                await self.pool_refreshed.wait()
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self.streams = 0
            log.info("games-by-users: pool refreshed; rebuilding streams")

    async def _stream_batch(self, index: int, ids: list[str]) -> None:
        det = self.settings.detection
        backoff = det.stream_backoff_seconds
        while True:
            started = time.monotonic()
            try:
                self.connections += 1
                log.info("games-by-users stream %d: opening for %d players", index, len(ids))
                async with aclosing(self.client.stream_games_by_users(ids, with_current_games=True)) as stream:
                    async for doc in stream:
                        self.docs += 1
                        self.handle(doc)
                log.warning("games-by-users stream %d ended; reconnecting in %.0fs", index, backoff)
            except asyncio.CancelledError:
                raise
            except LichessError as exc:
                log.warning("games-by-users stream %d failed (%s); reconnecting in %.0fs", index, exc, backoff)
            except Exception:
                log.exception("games-by-users stream %d crashed; reconnecting in %.0fs", index, backoff)
            if time.monotonic() - started >= 60:
                backoff = det.stream_backoff_seconds  # the connection was healthy for a while
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, det.stream_backoff_max_seconds)
