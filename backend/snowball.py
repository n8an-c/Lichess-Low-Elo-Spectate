"""Snowball pool builder: grow the pool from rated games instead of team rosters.

Team rosters are a poor source: members with a dormant sub-1000 blitz rating
mostly play variants, rapid or casual games. A player's recent rated games
(``GET /api/games/user/{id}``) name every opponent with the rating they had
in that game and the clock, so starting from a few seeds and following the
opponents of the configured time controls yields a pool of *proven* players
in minutes.

Measured on 2026-11-04: 31 requests / 111 s produced 604 players under 1000
with recent rated 3+0 games, versus 878 in 47 minutes from teams.

Every new member is confirmed through ``POST /api/users`` (closed accounts,
bots, provisional ratings and players who drifted above the cap are dropped).
"""

from __future__ import annotations

import heapq
import logging
import time
from contextlib import aclosing
from dataclasses import dataclass, field

from .config import Settings
from .db import Database, PlayerRow
from .lichess import LichessClient, LichessError, LichessHTTPError, LichessRateLimited
from .pool import confirm_members

log = logging.getLogger(__name__)

DAY_MS = 86_400_000


@dataclass
class Sighting:
    """What a user's game documents tell us about one player."""

    id: str
    name: str
    rating: int  # rating in the newest game seen (the pre-game rating at that time)
    last_game_at: int  # epoch ms of the newest matching game seen
    games: int = 0  # matching games seen
    provisional: bool = False
    bot: bool = False


@dataclass
class CrawlResult:
    requests: int = 0  # games-export requests (the crawl budget)
    profile_requests: int = 0  # POST /api/users confirmation calls
    players_expanded: int = 0
    games_seen: int = 0
    matching_games: int = 0
    opponents_seen: int = 0
    candidates: int = 0  # qualifying opponents not previously in the pool
    added: int = 0  # ...of which confirmed as members
    dropped: int = 0  # ...rejected by the profile check
    unconfirmed: int = 0  # ...left unconfirmed (profile budget / rate limit)
    rate_limit_hits: int = 0
    seconds: float = 0.0
    stopped: str = ""
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"crawl: {self.players_expanded} players' games fetched in {self.requests} requests, "
            f"{self.matching_games} matching games of {self.games_seen}, {self.opponents_seen} distinct opponents",
            f"members: {self.candidates} qualifying candidates, {self.added} added, {self.dropped} dropped by the "
            f"profile check ({self.profile_requests} look-up calls), {self.unconfirmed} unconfirmed",
            f"429 pauses: {self.rate_limit_hits}, elapsed: {self.seconds:.0f}s, stopped: {self.stopped or 'done'}",
        ]
        if self.errors:
            lines.append("errors: " + "; ".join(self.errors[:8]))
        return "\n".join(lines)


def collect_sightings(docs: list[dict], clock_pairs: set[tuple[int, int]], *, parent: str | None = None) -> tuple[dict[str, Sighting], Sighting | None, int]:
    """Keep rated standard games with an allowed clock; return (opponent sightings, parent's own sighting, matching games)."""
    sightings: dict[str, Sighting] = {}
    matching = 0
    for doc in docs:
        if not doc.get("rated") or doc.get("variant") != "standard":
            continue
        clock = doc.get("clock") or {}
        if (clock.get("initial"), clock.get("increment")) not in clock_pairs:
            continue
        matching += 1
        created = int(doc.get("createdAt") or 0)
        for side in ("white", "black"):
            p = (doc.get("players") or {}).get(side) or {}
            user = p.get("user") or {}
            uid, rating = user.get("id"), p.get("rating")
            if not uid or not isinstance(rating, int):
                continue  # anonymous or computer player
            s = sightings.get(uid)
            if s is None:
                s = Sighting(id=uid, name=user.get("name", uid), rating=rating, last_game_at=created)
                sightings[uid] = s
            s.games += 1
            if created >= s.last_game_at:
                s.last_game_at = created
                s.rating = rating
                s.name = user.get("name", s.name)
            s.provisional = s.provisional or bool(p.get("provisional"))
            s.bot = s.bot or user.get("title") == "BOT"
    own = sightings.pop(parent.lower(), None) if parent else None
    return sightings, own, matching


def qualifies_sighting(s: Sighting, rating_cap: int) -> bool:
    return not s.bot and not s.provisional and s.rating < rating_cap


class Crawler:
    def __init__(self, client: LichessClient, db: Database, settings: Settings):
        self.client = client
        self.db = db
        self.settings = settings
        self.clock_pairs = settings.detection.clock_pairs()
        self.perf_types = ",".join(sorted(settings.detection.speeds()))  # e.g. "blitz" or "blitz,rapid"
        self.last_result: CrawlResult | None = None

    # -- seeds -----------------------------------------------------------------------------

    async def seeds_from_arenas(self, limit: int = 20) -> list[str]:
        """Low-rated participants of Lichess's rating-capped arenas (a handful of requests).

        Arenas with one of our clocks come first, then other blitz/bullet
        capped arenas: the seeds only need to lead into the low-rated
        community, the crawl keeps just the games we want.
        """
        cap = self.settings.pool.rating_cap
        try:
            data = await self.client.current_tournaments()
        except LichessError as exc:
            log.warning("arena seed lookup failed: %s", exc)
            return []
        arenas = [a for key in ("started", "finished", "created") for a in (data.get(key) or [])]

        def arena_cap(a: dict) -> int:
            return (a.get("maxRating") or {}).get("rating", 10**9)

        def usable(a: dict) -> bool:
            clock = a.get("clock") or {}
            return (
                bool(a.get("rated"))
                and (a.get("variant") or {}).get("key") == "standard"
                and a.get("status") in (20, 30)  # started or finished: results exist
                and arena_cap(a) <= cap + 700
                and (clock.get("limit") or 0) <= 600
            )

        def same_clock(a: dict) -> bool:
            clock = a.get("clock") or {}
            return (clock.get("limit"), clock.get("increment")) in self.clock_pairs

        picks = sorted(
            (a for a in arenas if usable(a)),
            key=lambda a: (not same_clock(a), arena_cap(a), -(a.get("nbPlayers") or 0)),
        )[:3]
        seeds: list[tuple[int, str]] = []
        for arena in picks:
            try:
                async with aclosing(self.client.arena_results(arena["id"], nb=300)) as rows:
                    async for row in rows:
                        rating, name = row.get("rating"), row.get("username")
                        if isinstance(rating, int) and name and rating < cap + 300:
                            seeds.append((rating, name.lower()))
            except LichessError as exc:
                log.warning("arena %s results failed: %s", arena.get("id"), exc)
            if len(seeds) >= limit:
                break
        seeds.sort()
        found = list(dict.fromkeys(name for _, name in seeds))[:limit]
        if found:
            log.info("seeds: %d low-rated players from %d capped arena(s)", len(found), len(picks))
        else:
            log.info("seeds: no usable rating-capped arena right now (%d arenas listed)", len(arenas))
        return found

    async def seeds_from_teams(self, limit: int = 20, max_members: int = 600) -> list[str]:
        """Last resort: recently seen sub-cap members of the configured teams (one request per team)."""
        from .pool import qualifies  # local import: pool imports nothing from here

        pool = self.settings.pool
        found: list[str] = []
        for team_id in pool.team_ids:
            seen = 0
            try:
                async with aclosing(self.client.team_members(team_id, full=True)) as stream:
                    async for user in stream:
                        seen += 1
                        if qualifies(user, pool) and user["id"] not in found:
                            found.append(user["id"])
                        if len(found) >= limit or seen >= max_members:
                            break
            except LichessError as exc:
                log.warning("team %s seed lookup failed: %s", team_id, exc)
            if len(found) >= limit:
                break
        if found:
            log.info("seeds: %d sub-cap members from the configured teams", len(found))
        return found

    async def seeds(self, limit: int = 40) -> list[str]:
        """Config seeds, then unexpanded pool members, then arena participants, then team members."""
        seeds: list[str] = [s.lower() for s in self.settings.pool.seed_players]
        pool = self.settings.pool
        for pid in self.db.crawl_frontier(limit, prefer_rating_max=pool.expand_rating_max):
            if pid not in seeds:
                seeds.append(pid)
        if len(seeds) < 5:
            for pid in await self.seeds_from_arenas(limit):
                if pid not in seeds:
                    seeds.append(pid)
        if len(seeds) < 5:
            for pid in await self.seeds_from_teams(limit):
                if pid not in seeds:
                    seeds.append(pid)
        return seeds[:limit]

    # -- crawl -------------------------------------------------------------------------------

    async def crawl(
        self,
        *,
        request_budget: int,
        since_ms: int,
        target_members: int | None = None,
        parents: list[str] | None = None,
        expand: bool = True,
        now_ms: int | None = None,
    ) -> CrawlResult:
        """Fetch games of ``parents`` (or of the best seeds), add qualifying opponents, repeat.

        Stops when the request budget is spent, the frontier is empty, or the
        active pool reaches ``target_members``. With ``expand=False`` only the
        given parents are fetched (the periodic "trickle").
        """
        pool = self.settings.pool
        det = self.settings.detection
        started = time.monotonic()
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        result = CrawlResult()
        requests_before = self.client.request_count
        hits_before = self.client.rate_limit_hits

        # Frontier entries: (priority, id); low ratings and recent games first.
        frontier: list[tuple[tuple[int, int], str]] = []
        queued: set[str] = set()

        def push(pid: str, rating: int | None, last_game_at: int | None) -> None:
            if pid in queued or pid in visited:
                return
            queued.add(pid)
            prio = (0 if rating is not None and rating <= pool.expand_rating_max else 1, -(last_game_at or 0))
            heapq.heappush(frontier, (prio, pid))

        visited: set[str] = set()
        known = self.db.player_ids()
        active_ids = set(self.db.active_player_ids())
        seeds = [p.lower() for p in parents] if parents is not None else await self.seeds()
        seed_requests = self.client.request_count - requests_before  # arena look-ups, if any
        for pid in seeds:
            push(pid, None, None)
        if not frontier:
            result.stopped = "no seeds"
            result.seconds = time.monotonic() - started
            log.warning("crawl: nothing to start from (no seed_players, pool members or arena seeds)")
            return result

        to_confirm: list[str] = []
        opponents_seen: set[str] = set()

        async def confirm_pending() -> None:
            if not to_confirm:
                return
            batch, to_confirm[:] = list(to_confirm), []
            summary = await confirm_members(self.client, self.db, pool, batch, now_ms=now)
            result.profile_requests += summary.requests
            result.added += summary.kept
            result.dropped += summary.dropped
            result.unconfirmed += summary.unchecked
            if summary.kept:
                active_ids.update(summary.kept_ids)
            for pid in summary.dropped_ids:
                active_ids.discard(pid)
            if summary.rate_limited:
                result.stopped = "rate limited (profile check)"

        while frontier:
            if result.requests >= request_budget:
                result.stopped = f"request budget ({request_budget}) spent"
                break
            if target_members is not None and len(active_ids) + len(to_confirm) >= target_members:
                await confirm_pending()  # settle the candidates before deciding
                if result.stopped:
                    break
                if len(active_ids) >= target_members:
                    result.stopped = f"target of {target_members} members reached"
                    break
            (_, pid) = heapq.heappop(frontier)
            visited.add(pid)
            try:
                async with aclosing(
                    self.client.user_games(
                        pid, perf_type=self.perf_types, max_games=pool.games_per_request, since_ms=since_ms
                    )
                ) as stream:
                    docs = [doc async for doc in stream]
            except LichessRateLimited:
                result.stopped = "rate limited"
                break
            except LichessHTTPError as exc:
                result.requests += 1
                if exc.status_code == 404:
                    log.info("crawl: %s does not exist (closed account?); skipping", pid)
                    self.db.mark_expanded([pid], now)
                else:
                    result.errors.append(f"{pid}: HTTP {exc.status_code}")
                continue
            except LichessError as exc:
                result.requests += 1
                result.errors.append(f"{pid}: {exc}")
                continue
            result.requests += 1
            result.players_expanded += 1
            result.games_seen += len(docs)
            sightings, own, matching = collect_sightings(docs, self.clock_pairs, parent=pid)
            result.matching_games += matching
            if own is not None:
                self.db.update_player_activity(pid, last_game_at=own.last_game_at, game_rating=own.rating,
                                               games_recent=own.games, expanded_at=now)
                if pid not in known and qualifies_sighting(own, pool.rating_cap):
                    sightings[pid] = own  # a seed that qualifies joins the pool too
            else:
                self.db.mark_expanded([pid], now)
            new_rows: list[PlayerRow] = []
            for s in sightings.values():
                opponents_seen.add(s.id)
                if not qualifies_sighting(s, pool.rating_cap):
                    continue
                if expand:
                    push(s.id, s.rating, s.last_game_at)
                if s.id in known and s.id in active_ids:
                    self.db.update_player_activity(s.id, last_game_at=s.last_game_at, game_rating=s.rating)
                    continue
                # New, or dropped earlier: (re)join on this fresh evidence, pending the profile check.
                known.add(s.id)
                new_rows.append(PlayerRow(
                    id=s.id, username=s.name, blitz_rating=s.rating, provisional=False,
                    seen_at=s.last_game_at, source_team="games", last_game_at=s.last_game_at,
                    game_rating=s.rating, games_recent=s.games,
                ))
            if new_rows:
                self.db.upsert_players(new_rows, updated_at=now)
                result.candidates += len(new_rows)
                to_confirm.extend(r.id for r in new_rows)
            log.info(
                "crawl [%d/%d] %s: %d games, %d matching, %d opponents, %d new candidates (pool %d, frontier %d)",
                result.requests, request_budget, pid, len(docs), matching, len(sightings), len(new_rows),
                len(active_ids) + len(to_confirm), len(frontier),
            )
            if len(to_confirm) >= 300:
                await confirm_pending()
                if result.stopped:
                    break
        else:
            result.stopped = "frontier exhausted"
        await confirm_pending()
        result.opponents_seen = len(opponents_seen)
        result.rate_limit_hits = self.client.rate_limit_hits - hits_before
        result.seconds = time.monotonic() - started
        if self.client.request_count - requests_before != result.requests + result.profile_requests + seed_requests:
            log.debug("crawl request accounting: client saw %d requests", self.client.request_count - requests_before)
        self.last_result = result
        log.info("crawl finished: %s", result.summary().replace("\n", " | "))
        return result


def since_days_ms(now_ms: int, days: float) -> int:
    return int(now_ms - days * DAY_MS)
