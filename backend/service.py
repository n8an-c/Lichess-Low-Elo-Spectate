"""Orchestrates the pool refresher, poller and verifier in one process."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

from .config import Settings
from .db import Database
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
        self.candidates = Candidates(settings.detection.candidate_ttl_seconds)
        self.poller = Poller(self.client, self.db, settings.detection, self.queue)
        self.on_accept: list[AcceptCallback] = []
        self.accepted_total = 0
        self.rejected_total = 0
        self.crashes = 0
        self.restart_delay = 5.0
        self._tasks: list[asyncio.Task] = []

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
                log.info("player pool is empty or stale; building it now (polling continues meanwhile)")
                try:
                    result = await build_pool(self.client, self.db, self.settings.pool)
                    log.info("pool refresh done: %d active players", result.qualifying)
                except Exception:  # keep the service alive; try again next cycle
                    log.exception("pool build failed")
            await asyncio.sleep(refresh_seconds)
            self.skip_pool_build = False

    async def verifier_loop(self) -> None:
        while True:
            game_id = await self.queue.get()
            try:
                verdict = await verify_game(self.client, game_id, self.settings)
                self.db.record_game(game_id, verdict.accepted)
                if verdict.accepted and verdict.game is not None:
                    self.accepted_total += 1
                    self.candidates.add(verdict.game)
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
                self.poller.release(game_id)
                self.queue.task_done()

    async def expiry_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            for game in self.candidates.expire():
                log.debug("candidate %s expired (older than %.0fs)", game.id, self.candidates.ttl)

    # -- lifecycle -----------------------------------------------------------------

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
        loops = {
            "pool": self.pool_loop,
            "poller": self.poller.run,
            "verifier": self.verifier_loop,
            "expiry": self.expiry_loop,
        }
        self._tasks = [
            asyncio.create_task(self._supervise(name, factory, self.restart_delay), name=name)
            for name, factory in loops.items()
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
