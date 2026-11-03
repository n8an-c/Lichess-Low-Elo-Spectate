"""Pool builder: find low-rated, recently active players and store them in SQLite.

Flow:
1. Collect teams: configured IDs plus keyword search results.
2. Stream each team's members with ``full=true`` (full user docs, capped at
   the 1,000 most recent members per team) and keep the ones that qualify.
3. Re-check previously pooled players that no team scan returned this run via
   POST /api/users (300 per call, capped per run to respect Lichess quotas).
4. Upsert qualifying players; deactivate evaluated players that no longer qualify.
"""

from __future__ import annotations

import logging
import time
from contextlib import aclosing
from dataclasses import dataclass, field

from .config import PoolSettings
from .db import Database, PlayerRow, TeamRow
from .lichess import LichessClient, LichessHTTPError, LichessRateLimited

log = logging.getLogger(__name__)

PROFILE_BATCH = 300


@dataclass
class PoolBuildResult:
    teams_found: int = 0
    teams_scanned: int = 0
    teams_skipped: int = 0
    users_seen: int = 0
    qualifying: int = 0
    added: int = 0
    deactivated: int = 0
    refreshed: int = 0
    refresh_truncated: int = 0
    discovered_fetched: int = 0
    discovered_added: int = 0
    discovered_pending: int = 0
    rate_limit_hits: int = 0
    requests: int = 0
    seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"teams: {self.teams_found} found, {self.teams_scanned} scanned, {self.teams_skipped} skipped",
            f"users evaluated: {self.users_seen} (team scans) + {self.refreshed} (profile refresh)",
            f"qualifying players: {self.qualifying} ({self.added} new), deactivated: {self.deactivated}",
            f"opponents: {self.discovered_fetched} fetched, {self.discovered_added} added, "
            f"{self.discovered_pending} waiting for the next refresh",
            f"lichess requests: {self.requests}, 429 pauses: {self.rate_limit_hits}, elapsed: {self.seconds:.0f}s",
        ]
        if self.refresh_truncated:
            lines.append(
                f"NOTE: {self.refresh_truncated} previously pooled players were not re-checked "
                "(refresh cap or rate limit); they keep their previous state"
            )
        if self.errors:
            lines.append("errors: " + "; ".join(self.errors))
        return "\n".join(lines)


def qualifies(user: dict, settings: PoolSettings, now_ms: int | None = None) -> bool:
    """Apply the spec's rating rule to a Lichess user document."""
    if user.get("disabled") or user.get("tosViolation"):
        return False
    if user.get("title") == "BOT":
        return False
    blitz = (user.get("perfs") or {}).get("blitz")
    if not blitz or "rating" not in blitz:
        return False
    if blitz.get("prov"):
        return False
    if blitz["rating"] >= settings.rating_cap:
        return False
    seen_at = user.get("seenAt")
    if seen_at is None:
        return False
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    return seen_at >= now - settings.active_days * 86_400_000


def to_player_row(user: dict, source_team: str | None = None) -> PlayerRow:
    blitz = user["perfs"]["blitz"]
    return PlayerRow(
        id=user["id"],
        username=user.get("username") or user.get("name") or user["id"],
        blitz_rating=int(blitz["rating"]),
        provisional=bool(blitz.get("prov", False)),
        seen_at=int(user["seenAt"]),
        source_team=source_team,
    )


async def collect_teams(client: LichessClient, settings: PoolSettings, result: PoolBuildResult) -> list[TeamRow]:
    teams: dict[str, TeamRow] = {}
    for team_id in settings.team_ids:
        teams[team_id] = TeamRow(id=team_id, name=team_id, source="config")
    for keyword in settings.team_keywords:
        page = 1
        while page is not None and page <= settings.keyword_max_pages:
            try:
                data = await client.search_teams(keyword, page)
            except LichessHTTPError as exc:
                msg = f"team search '{keyword}' page {page}: HTTP {exc.status_code}"
                log.warning(msg)
                result.errors.append(msg)
                break
            for team in data.get("currentPageResults", []):
                if team["id"] not in teams:
                    teams[team["id"]] = TeamRow(
                        id=team["id"],
                        name=team.get("name", team["id"]),
                        source=f"keyword:{keyword}",
                        nb_members=team.get("nbMembers"),
                    )
            log.info(
                "keyword '%s' page %d/%s: %d teams so far",
                keyword, page, data.get("nbPages"), len(teams),
            )
            page = data.get("nextPage")
    result.teams_found = len(teams)
    return list(teams.values())


async def scan_team(
    client: LichessClient, team: TeamRow, settings: PoolSettings, result: PoolBuildResult, now_ms: int
) -> tuple[dict[str, PlayerRow], set[str]]:
    """Return (qualifying rows by id, all evaluated ids) for one team."""
    qualifying: dict[str, PlayerRow] = {}
    evaluated: set[str] = set()
    # aclosing() closes the stream (and releases the client's lock) when we stop early.
    async with aclosing(client.team_members(team.id, full=True)) as stream:
        async for user in stream:
            uid = user.get("id")
            if not uid:
                continue
            evaluated.add(uid)
            if qualifies(user, settings, now_ms):
                qualifying[uid] = to_player_row(user, team.id)
            if len(evaluated) >= settings.max_members_per_team:
                log.info("%s: reached max_members_per_team (%d), stopping scan", team.id, len(evaluated))
                break
    return qualifying, evaluated


async def build_pool(
    client: LichessClient,
    db: Database,
    settings: PoolSettings,
    now_ms: int | None = None,
    max_teams: int | None = None,
) -> PoolBuildResult:
    """Run one full pool build. ``max_teams`` limits the scan (useful for a quick trial run).

    Qualifying players are written after every team, so interrupting a run
    still leaves a usable (partial) pool; deactivation happens once at the end.
    """
    started = time.monotonic()
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    result = PoolBuildResult()
    requests_before = client.request_count
    hits_before = client.rate_limit_hits

    teams = await collect_teams(client, settings, result)
    db.upsert_teams(teams)
    if max_teams is not None:
        teams = teams[:max_teams]
    log.info("scanning %d teams", len(teams))

    known_before = db.player_ids()
    qualifying: dict[str, PlayerRow] = {}
    evaluated: set[str] = set()

    for i, team in enumerate(teams, 1):
        try:
            team_q, team_e = await scan_team(client, team, settings, result, now_ms)
        except LichessHTTPError as exc:
            result.teams_skipped += 1
            msg = f"team {team.id}: HTTP {exc.status_code}"
            log.warning("skipping %s", msg)
            result.errors.append(msg)
            continue
        except LichessRateLimited as exc:
            result.teams_skipped += 1
            log.error("team %s: %s; skipping", team.id, exc)
            result.errors.append(f"team {team.id}: rate limited")
            continue
        result.teams_scanned += 1
        evaluated |= team_e
        new_rows = [row for uid, row in team_q.items() if uid not in qualifying]
        qualifying.update({row.id: row for row in new_rows})
        db.upsert_players(new_rows, updated_at=now_ms)
        db.mark_team_scanned(team.id)
        log.info(
            "[%d/%d] %s: %d members streamed, %d qualify (pool so far %d)",
            i, len(teams), team.id, len(team_e), len(team_q), len(qualifying),
        )
    result.users_seen = len(evaluated)

    # Re-check previously active players that no team scan covered this run.
    stale = [pid for pid in db.active_player_ids() if pid not in evaluated]
    if stale:
        log.info("refreshing %d previously pooled players not covered by team scans", len(stale))
    batches = [stale[i : i + PROFILE_BATCH] for i in range(0, len(stale), PROFILE_BATCH)]
    profile_batches_used = 0
    profile_fetch_blocked = False  # rate limited / HTTP error: do not spend more quota this run
    for b, batch in enumerate(batches):
        if b >= settings.max_profile_refresh_batches:
            log.warning("profile refresh cap reached (%d batches)", settings.max_profile_refresh_batches)
            break
        try:
            users = await client.users_by_ids(batch)
        except LichessRateLimited:
            log.error("rate limited during profile refresh; leaving remaining players untouched")
            result.errors.append("profile refresh rate limited")
            profile_fetch_blocked = True
            break
        except LichessHTTPError as exc:
            log.warning("profile refresh batch %d failed: HTTP %s", b, exc.status_code)
            result.errors.append(f"profile refresh batch {b}: HTTP {exc.status_code}")
            profile_fetch_blocked = True
            break
        profile_batches_used += 1
        returned = {u["id"] for u in users if "id" in u}
        # IDs Lichess did not return no longer exist (closed accounts); count them as evaluated.
        evaluated |= set(batch)
        result.refreshed += len(batch)
        new_rows = [to_player_row(u) for u in users if qualifies(u, settings, now_ms) and u["id"] not in qualifying]
        qualifying.update({row.id: row for row in new_rows})
        db.upsert_players(new_rows, updated_at=now_ms)
        missing = set(batch) - returned
        if missing:
            log.info("%d refreshed players no longer exist on Lichess", len(missing))
    result.refresh_truncated = sum(len(b) for b in batches) - result.refreshed

    # Opponents discovered from game headers: look them up with whatever
    # profile budget is left this run (same 8,000-per-10-minutes quota).
    discovered = [] if profile_fetch_blocked else db.discovered_to_fetch()
    remaining_batches = max(0, settings.max_profile_refresh_batches - profile_batches_used)
    d_batches = [discovered[i : i + PROFILE_BATCH] for i in range(0, len(discovered), PROFILE_BATCH)]
    if discovered:
        log.info("looking up %d discovered opponents (%d batches available)", len(discovered), remaining_batches)
    for b, batch in enumerate(d_batches):
        if b >= remaining_batches:
            log.info("profile budget used up; remaining discovered opponents wait for the next refresh")
            break
        try:
            users = await client.users_by_ids(batch)
        except LichessRateLimited:
            log.error("rate limited while fetching discovered opponents; stopping for this run")
            result.errors.append("opponent lookup rate limited")
            break
        except LichessHTTPError as exc:
            log.warning("opponent lookup batch %d failed: HTTP %s", b, exc.status_code)
            result.errors.append(f"opponent lookup batch {b}: HTTP {exc.status_code}")
            break
        result.discovered_fetched += len(batch)
        new_rows = [
            to_player_row(u, "opponent") for u in users if qualifies(u, settings, now_ms) and u["id"] not in qualifying
        ]
        qualifying.update({row.id: row for row in new_rows})
        result.discovered_added += len(new_rows)
        db.upsert_players(new_rows, updated_at=now_ms)
        db.mark_discovered_fetched(batch)
        log.info("opponent batch %d: %d looked up, %d qualify", b + 1, len(batch), len(new_rows))
    result.discovered_pending = len(discovered) - result.discovered_fetched

    to_deactivate = [pid for pid in evaluated if pid not in qualifying]
    result.deactivated = db.deactivate_players(to_deactivate, updated_at=now_ms)
    result.qualifying = len(qualifying)
    result.added = len(set(qualifying) - known_before)
    result.requests = client.request_count - requests_before
    result.rate_limit_hits = client.rate_limit_hits - hits_before
    result.seconds = time.monotonic() - started
    log.info("pool build finished in %.0fs", result.seconds)
    return result
