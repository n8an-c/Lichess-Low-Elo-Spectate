"""Orchestrates the pool refresher, the detectors and the verifier in one process."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable

from .config import Settings
from .db import Database
from .gamestream import GameStreamWatcher
from .lichess import LichessClient
from .poller import Poller
from .pool import build_pool
from .verifier import Candidates, GameInfo, verify_game

log = logging.getLogger(__name__)

AcceptCallback = Callable[[GameInfo], None]


class Spectator:
    def __init__(self, settings: Settings, *, skip_pool_build: bool = False, client: LichessClient | None = None):
        self.settings = settings
        self.skip_pool_build = skip_pool_build
        self.client = client or LichessClient(settings.lichess)
        self.db = Database(settings.storage.sqlite_path)
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.pending: set[str] = set()  # game IDs queued or being verified (shared by the detectors)
        self.pool_refreshed = asyncio.Event()
        self.candidates = Candidates(settings.detection.candidate_ttl_seconds)
        det = settings.detection
        self.poller = Poller(self.client, self.db, det, self.queue, self.pending) if det.poll_enabled else None
        self.watcher = (
            GameStreamWatcher(self.client, self.db, settings, self.queue, self.pending, self.pool_refreshed, self.discover)
            if det.stream_enabled
            else None
        )
        self.on_accept: list[AcceptCallback] = []
        self.recent: deque[GameInfo] = deque(maxlen=200)  # accepted games, newest first
        self.accepted_total = 0
        self.rejected_total = 0
        self.discovered_total = 0
        self.crashes = 0
        self.restart_delay = 5.0
        self.started_at = time.time()
        self._pool_size_cache: tuple[float, int] = (0.0, 0)
        self._tasks: list[asyncio.Task] = []

    # -- feed for the web page -------------------------------------------------------

    def record_accepted(self, game: GameInfo) -> None:
        self.recent.appendleft(game)
        self.accepted_total += 1
        self.candidates.add(game)

    def pool_size(self) -> int:
        at, size = self._pool_size_cache
        now = time.monotonic()
        if now - at > 5:
            try:
                size = len(self.db.active_player_ids())
            except Exception:  # closed database at shutdown: keep the last value
                pass
            self._pool_size_cache = (now, size)
        return size

    def stats(self) -> dict:
        disc = self.db.discovered_stats() if self.db.conn else {"total": 0, "pending": 0}
        return {
            "mode": self.settings.detection.mode,
            "poolSize": self.pool_size(),
            "accepted": self.accepted_total,
            "rejected": self.rejected_total,
            "pollPasses": self.poller.passes if self.poller else None,
            "streams": self.watcher.streams if self.watcher else None,
            "discoveredPending": disc["pending"],
            "uptimeSeconds": round(time.time() - self.started_at),
        }

    def feed(self, limit: int = 50) -> dict:
        """Recent accepted games (newest first) plus service stats, for GET /api/games."""
        now = time.time()
        return {"games": [g.to_dict(now) for g in list(self.recent)[:limit]], "stats": self.stats(), "now": now}

    # -- opponent discovery --------------------------------------------------------

    def discover(self, player_id: str, rating: int) -> None:
        """Record an opponent seen in a game header as a pool candidate."""
        pool = self.settings.pool
        if rating >= pool.rating_cap or self.db.in_pool(player_id, pool.active_days):
            return
        self.db.record_discovered(player_id, rating, pool.active_days)
        self.discovered_total += 1
        log.debug("discovered opponent %s (%d)", player_id, rating)

    # -- background loops --------------------------------------------------------

    def pool_is_fresh(self) -> bool:
        last = self.db.pool_last_updated()
        if last is None:
            return False
        age_hours = (time.time() * 1000 - last) / 3_600_000
        return age_hours < self.settings.pool.pool_refresh_hours

    async def pool_loop(self) -> None:
        refresh_seconds = self.settings.pool.pool_refresh_hours * 3600
        while True:
            if self.skip_pool_build and self.db.pool_last_updated() is None:
                log.warning("--skip-pool-build given but the pool is empty; run build-pool first")
            elif self.pool_is_fresh() or self.skip_pool_build:
                log.info("player pool is fresh (%d active players); next refresh in %.1fh",
                         len(self.db.active_player_ids()), self.settings.pool.pool_refresh_hours)
            else:
                log.info("player pool is empty or stale; building it now (detection continues meanwhile)")
                try:
                    result = await build_pool(self.client, self.db, self.settings.pool)
                    log.info("pool refresh done: %d active players", result.qualifying)
                    self.pool_refreshed.set()  # the games-by-users watcher rebuilds its streams
                except Exception:  # keep the service alive; try again next cycle
                    log.exception("pool build failed")
            await asyncio.sleep(refresh_seconds)
            self.skip_pool_build = False

    async def verifier_loop(self) -> None:
        while True:
            game_id = await self.queue.get()
            try:
                verdict = await verify_game(self.client, game_id, self.settings, discover=self.discover)
                self.db.record_game(game_id, verdict.accepted)
                if verdict.accepted and verdict.game is not None:
                    self.record_accepted(verdict.game)
                    log.info("ACCEPTED %s  %s", verdict.game.describe(), verdict.game.url)
                    for cb in self.on_accept:
                        try:
                            cb(verdict.game)
                        except Exception:
                            log.exception("on_accept callback failed")
                else:
                    self.rejected_total += 1
                    log.info("rejected %s: %s", game_id, verdict.reason)
            except Exception:
                log.exception("verification of %s crashed", game_id)
            finally:
                self.pending.discard(game_id)
                self.queue.task_done()

    async def expiry_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            for game in self.candidates.expire():
                log.debug("candidate %s expired (older than %.0fs)", game.id, self.candidates.ttl)

    # -- lifecycle -----------------------------------------------------------------

    def loops(self) -> dict[str, Callable]:
        """The background loops enabled by the configuration."""
        loops: dict[str, Callable] = {"pool": self.pool_loop}
        if self.poller is not None:
            loops["poller"] = self.poller.run
        if self.watcher is not None:
            loops["gamestream"] = self.watcher.run
        loops["verifier"] = self.verifier_loop
        loops["expiry"] = self.expiry_loop
        return loops

    async def _supervise(self, name: str, factory, restart_delay: float = 5.0) -> None:
        """Run a loop forever; log and restart it if it crashes. Only cancellation stops it."""
        while True:
            try:
                await factory()
                log.error("%s loop returned unexpectedly; restarting in %.0fs", name, restart_delay)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("%s loop crashed; restarting in %.0fs", name, restart_delay)
                self.crashes += 1
            await asyncio.sleep(restart_delay)

    async def run(self) -> None:
        """Run all loops until cancelled. A crashing loop is restarted, never fatal."""
        log.info("detection mode: %s", self.settings.detection.mode)
        self._tasks = [
            asyncio.create_task(self._supervise(name, factory, self.restart_delay), name=name)
            for name, factory in self.loops().items()
        ]
        try:
            await asyncio.gather(*self._tasks)
        finally:
            await self.stop()

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks = []
        await self.client.aclose()
        self.db.close()
