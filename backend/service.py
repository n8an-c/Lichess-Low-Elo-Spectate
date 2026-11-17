"""Orchestrates the pool builders, the detectors and the verifier in one process."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Callable

from .config import Settings
from .db import Database, now_ms
from .gamestream import GameStreamWatcher
from .lichess import LichessClient
from .poller import Poller
from .pool import build_pool, confirm_members, fetch_discovered
from .snowball import Crawler, since_days_ms
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
        self.crawler = Crawler(self.client, self.db, settings) if settings.pool.games_enabled else None
        self.on_accept: list[AcceptCallback] = []
        self.recent: deque[GameInfo] = deque(maxlen=200)  # accepted games, newest first
        self.accepted_total = 0
        self.rejected_total = 0
        self.discovered_total = 0
        self.crashes = 0
        self.restart_delay = 5.0
        self.started_at = time.time()
        self.building = False
        self._pool_lock = asyncio.Lock()  # build / top-up, trickle and refresh never run at the same time
        self._pool_size_cache: tuple[float, int] = (0.0, 0)
        self._tasks: list[asyncio.Task] = []
        self._verify_tasks: set[asyncio.Task] = set()

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
                size = self.db.active_count()
            except Exception:  # closed database at shutdown: keep the last value
                pass
            self._pool_size_cache = (now, size)
        return size

    def stats(self) -> dict:
        try:
            disc = self.db.discovered_stats()
            activity = self.db.activity_stats()
        except Exception:
            disc, activity = {"total": 0, "pending": 0}, {}
        det = self.settings.detection
        crawl = self.crawler.last_result if self.crawler else None
        return {
            "mode": det.mode,
            "poolSource": self.settings.pool.source,
            "poolSize": self.pool_size(),
            "activeLastDay": activity.get("last_day"),
            "hotPlayers": len(self.poller.hot) if self.poller else None,
            "accepted": self.accepted_total,
            "rejected": self.rejected_total,
            "pollPasses": self.poller.passes if self.poller else None,
            "sweepSeconds": round(self.poller.last_sweep_seconds) if self.poller else None,
            "requestsPerMinute": self.client.requests_per_minute(),
            "rateLimitHits": self.client.rate_limit_hits,
            "streams": self.watcher.streams if self.watcher else None,
            "discoveredPending": disc["pending"],
            "building": self.building,
            "lastCrawl": None if crawl is None else {
                "requests": crawl.requests, "added": crawl.added, "dropped": crawl.dropped,
                "seconds": round(crawl.seconds), "stopped": crawl.stopped,
            },
            "timeControls": det.time_controls,
            "ratingRule": det.rating_rule,
            "ratingCap": self.settings.pool.rating_cap,
            "maxStartPlies": det.max_start_plies,
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

    # -- pool loops --------------------------------------------------------------------

    def pool_is_fresh(self) -> bool:
        last = self.db.pool_last_updated()
        if last is None:
            return False
        age_hours = (time.time() * 1000 - last) / 3_600_000
        return age_hours < self.settings.pool.pool_refresh_hours

    async def build_or_top_up(self) -> None:
        """Teams scan (if enabled and stale) and/or snowball crawl up to the member target."""
        async with self._pool_lock:
            await self._build_or_top_up()

    async def _build_or_top_up(self) -> None:
        pool = self.settings.pool
        self.building = True
        try:
            if pool.teams_enabled and not self.pool_is_fresh():
                log.info("team scan: pool is empty or stale; scanning teams (detection continues meanwhile)")
                result = await build_pool(self.client, self.db, pool)
                log.info("team scan done: %d active players", result.qualifying)
            if self.crawler is not None:
                active = self.db.active_count()
                if active < pool.max_members:
                    log.info("pool has %d members (target %d); crawling rated games for more", active, pool.max_members)
                    now = now_ms()
                    result = await self.crawler.crawl(
                        request_budget=pool.crawl_request_budget,
                        since_ms=since_days_ms(now, pool.games_since_days),
                        target_members=pool.max_members,
                        now_ms=now,
                    )
                    log.info("crawl done: %d active players", self.db.active_count())
                else:
                    log.info("pool has %d members (target %d); no crawl needed", active, pool.max_members)
            self.pool_refreshed.set()  # the games-by-users watcher rebuilds its streams
        except Exception:  # keep the service alive; try again next cycle
            log.exception("pool build failed")
        finally:
            self.building = False

    async def pool_loop(self) -> None:
        refresh_seconds = self.settings.pool.pool_refresh_hours * 3600
        while True:
            if self.skip_pool_build and self.db.pool_last_updated() is None:
                log.warning("--skip-pool-build given but the pool is empty; run build-pool first")
            elif self.skip_pool_build:
                log.info("--skip-pool-build: keeping the %d active players as they are", self.db.active_count())
            else:
                await self.build_or_top_up()
            await asyncio.sleep(refresh_seconds)
            self.skip_pool_build = False

    async def trickle_loop(self) -> None:
        """Every few minutes re-read the latest games of the most active members for fresh opponents."""
        pool = self.settings.pool
        while True:
            await asyncio.sleep(pool.trickle_interval_minutes * 60)
            if self.crawler is None or self._pool_lock.locked():
                continue  # a build or refresh is running; try again next time
            try:
                async with self._pool_lock:
                    await self._trickle_once()
            except Exception:
                log.exception("trickle crawl failed")

    async def _trickle_once(self) -> None:
        pool = self.settings.pool
        if self.db.active_count() >= pool.max_members * 1.25:
            return  # expiry will trim the pool first
        parents = self.db.most_active_members(pool.trickle_players)
        if self.poller is not None:
            for pid in sorted(self.poller.hot, key=self.poller.hot.get, reverse=True):
                if len(parents) >= pool.trickle_players * 2:
                    break
                if pid not in parents:
                    parents.append(pid)
        if not parents:
            return
        now = now_ms()
        result = await self.crawler.crawl(
            request_budget=len(parents),
            since_ms=int(now - pool.trickle_hours * 3_600_000),
            parents=parents,
            expand=False,
            now_ms=now,
        )
        if result.added:
            log.info("trickle: %d new members from %d players' latest games", result.added, result.players_expanded)
            self.pool_refreshed.set()

    async def refresh_loop(self) -> None:
        """Light refresh: re-check stale profiles, look up discovered opponents, expire inactive members."""
        pool = self.settings.pool
        while True:
            await asyncio.sleep(pool.light_refresh_minutes * 60)
            if self._pool_lock.locked():
                continue
            try:
                async with self._pool_lock:
                    await self._refresh_once()
            except Exception:
                log.exception("light refresh failed")

    async def _refresh_once(self) -> None:
        pool = self.settings.pool
        now = now_ms()
        changed = False
        budget = pool.max_profile_refresh_batches
        stale = self.db.members_to_recheck(now - int(pool.profile_recheck_hours * 3_600_000), limit=budget * 300)
        if stale:
            summary = await confirm_members(self.client, self.db, pool, stale, now_ms=now, max_batches=budget)
            budget -= summary.requests
            log.info("profile refresh: %d checked, %d kept, %d dropped", summary.checked, summary.kept, summary.dropped)
            changed = changed or summary.dropped > 0
            if summary.rate_limited:
                log.warning("profile refresh was rate limited; skipping the rest of this refresh")
                budget = 0
        if budget > 0:
            fetched, added, waiting = await fetch_discovered(self.client, self.db, pool, now, budget)
            if fetched:
                log.info("discovered opponents: %d looked up, %d added, %d waiting", fetched, added, waiting)
                changed = changed or added > 0
        if pool.games_enabled:
            expired = self.db.expire_inactive_members(since_days_ms(now, pool.games_since_days), updated_at=now)
            if expired:
                log.info("expired %d members with no game in %d days", expired, pool.games_since_days)
                changed = True
        if changed:
            self.pool_refreshed.set()

    # -- verification ------------------------------------------------------------------

    async def verifier_loop(self) -> None:
        sem = asyncio.Semaphore(self.settings.detection.verify_concurrency)
        while True:
            game_id = await self.queue.get()
            await sem.acquire()
            task = asyncio.create_task(self._verify_one(game_id, sem), name=f"verify-{game_id}")
            self._verify_tasks.add(task)
            task.add_done_callback(self._verify_tasks.discard)

    async def _verify_one(self, game_id: str, sem: asyncio.Semaphore) -> None:
        try:
            verdict = await verify_game(self.client, game_id, self.settings, discover=self.discover)
            if verdict.transient:
                # Not recorded in seen_games: the poller re-queues it if it is still being played.
                log.warning("could not verify %s (%s); will retry if it is seen again", game_id, verdict.reason)
                return
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
            sem.release()

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
        if self.crawler is not None:
            loops["trickle"] = self.trickle_loop
        loops["refresh"] = self.refresh_loop
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
        det = self.settings.detection
        log.info(
            "detection mode: %s; pool source: %s; time controls: %s; rating rule: %s under %d; max start plies: %d",
            det.mode, self.settings.pool.source, det.label, det.rating_rule, self.settings.pool.rating_cap, det.max_start_plies,
        )
        self._tasks = [
            asyncio.create_task(self._supervise(name, factory, self.restart_delay), name=name)
            for name, factory in self.loops().items()
        ]
        try:
            await asyncio.gather(*self._tasks)
        finally:
            await self.stop()

    async def stop(self) -> None:
        for task in [*self._tasks, *self._verify_tasks]:
            task.cancel()
        for task in [*self._tasks, *self._verify_tasks]:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks = []
        await self.client.aclose()
        self.db.close()
