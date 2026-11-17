"""SQLite storage for teams, pooled players and seen games."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS teams (
    id           TEXT PRIMARY KEY,
    name         TEXT,
    source       TEXT,
    nb_members   INTEGER,
    last_scanned INTEGER
);

CREATE TABLE IF NOT EXISTS players (
    id           TEXT PRIMARY KEY,
    username     TEXT,
    blitz_rating INTEGER,
    provisional  INTEGER NOT NULL DEFAULT 0,
    seen_at      INTEGER,
    active       INTEGER NOT NULL DEFAULT 1,
    updated_at   INTEGER NOT NULL,
    source_team  TEXT
);
CREATE INDEX IF NOT EXISTS players_active_idx ON players(active);

CREATE TABLE IF NOT EXISTS seen_games (
    id         TEXT PRIMARY KEY,
    first_seen INTEGER NOT NULL,
    accepted   INTEGER NOT NULL DEFAULT 0
);

-- Opponents seen in game headers who are not in the pool yet; the pool
-- refresh fetches their profiles and adds the ones that qualify.
CREATE TABLE IF NOT EXISTS discovered_players (
    id          TEXT PRIMARY KEY,
    first_seen  INTEGER NOT NULL,
    last_seen   INTEGER NOT NULL,
    last_rating INTEGER,
    fetched_at  INTEGER
);
"""

DAY_MS = 86_400_000

# Columns added after the first release; created on the fly for older databases.
PLAYER_COLUMNS = {
    "last_game_at": "INTEGER",  # epoch ms of the newest matching rated game seen (games source)
    "game_rating": "INTEGER",  # the player's rating in that game
    "games_recent": "INTEGER",  # matching games seen in the crawl window
    "expanded_at": "INTEGER",  # when the crawler last fetched this player's games
}


@dataclass(frozen=True)
class PlayerRow:
    id: str
    username: str
    blitz_rating: int
    provisional: bool
    seen_at: int  # epoch milliseconds, as Lichess reports it
    source_team: str | None = None
    last_game_at: int | None = None
    game_rating: int | None = None
    games_recent: int | None = None


@dataclass(frozen=True)
class TeamRow:
    id: str
    name: str
    source: str
    nb_members: int | None = None


def now_ms() -> int:
    return int(time.time() * 1000)


class Database:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self._migrate()

    def _migrate(self) -> None:
        existing = {row[1] for row in self.conn.execute("PRAGMA table_info(players)")}
        with self.conn:
            for column, kind in PLAYER_COLUMNS.items():
                if column not in existing:
                    self.conn.execute(f"ALTER TABLE players ADD COLUMN {column} {kind}")

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # -- teams ---------------------------------------------------------------------

    def upsert_teams(self, teams: Iterable[TeamRow]) -> None:
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO teams (id, name, source, nb_members)
                VALUES (:id, :name, :source, :nb_members)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    nb_members = COALESCE(excluded.nb_members, teams.nb_members)
                """,
                [t.__dict__ for t in teams],
            )

    def mark_team_scanned(self, team_id: str, when_ms: int | None = None) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE teams SET last_scanned = ? WHERE id = ?", (when_ms or now_ms(), team_id)
            )

    # -- players -------------------------------------------------------------------

    def upsert_players(self, rows: Iterable[PlayerRow], updated_at: int | None = None) -> None:
        ts = updated_at or now_ms()
        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO players
                    (id, username, blitz_rating, provisional, seen_at, active, updated_at, source_team,
                     last_game_at, game_rating, games_recent)
                VALUES (:id, :username, :blitz_rating, :provisional, :seen_at, 1, :updated_at, :source_team,
                        :last_game_at, :game_rating, :games_recent)
                ON CONFLICT(id) DO UPDATE SET
                    username = excluded.username,
                    blitz_rating = excluded.blitz_rating,
                    provisional = excluded.provisional,
                    seen_at = MAX(excluded.seen_at, COALESCE(players.seen_at, 0)),
                    active = 1,
                    updated_at = excluded.updated_at,
                    source_team = COALESCE(excluded.source_team, players.source_team),
                    game_rating = CASE
                        WHEN excluded.last_game_at IS NOT NULL
                             AND excluded.last_game_at >= COALESCE(players.last_game_at, 0)
                        THEN excluded.game_rating ELSE COALESCE(players.game_rating, excluded.game_rating) END,
                    last_game_at = CASE
                        WHEN excluded.last_game_at IS NULL THEN players.last_game_at
                        WHEN players.last_game_at IS NULL THEN excluded.last_game_at
                        ELSE MAX(excluded.last_game_at, players.last_game_at) END,
                    games_recent = COALESCE(excluded.games_recent, players.games_recent)
                """,
                [
                    {
                        "id": r.id,
                        "username": r.username,
                        "blitz_rating": r.blitz_rating,
                        "provisional": int(r.provisional),
                        "seen_at": r.seen_at,
                        "updated_at": ts,
                        "source_team": r.source_team,
                        "last_game_at": r.last_game_at,
                        "game_rating": r.game_rating,
                        "games_recent": r.games_recent,
                    }
                    for r in rows
                ],
            )

    def update_player_activity(
        self,
        player_id: str,
        *,
        last_game_at: int | None = None,
        game_rating: int | None = None,
        games_recent: int | None = None,
        expanded_at: int | None = None,
    ) -> None:
        """Record what a games export told us about a player (newest game wins)."""
        with self.conn:
            self.conn.execute(
                """
                UPDATE players SET
                    game_rating = CASE
                        WHEN :last_game_at IS NOT NULL AND :last_game_at >= COALESCE(last_game_at, 0)
                             AND :game_rating IS NOT NULL
                        THEN :game_rating ELSE game_rating END,
                    last_game_at = CASE
                        WHEN :last_game_at IS NOT NULL AND :last_game_at > COALESCE(last_game_at, 0)
                        THEN :last_game_at ELSE last_game_at END,
                    games_recent = COALESCE(:games_recent, games_recent),
                    expanded_at = COALESCE(:expanded_at, expanded_at)
                WHERE id = :id
                """,
                {"id": player_id, "last_game_at": last_game_at, "game_rating": game_rating,
                 "games_recent": games_recent, "expanded_at": expanded_at},
            )

    def mark_expanded(self, ids: Iterable[str], when_ms: int | None = None) -> None:
        ts = when_ms or now_ms()
        with self.conn:
            self.conn.executemany("UPDATE players SET expanded_at = ? WHERE id = ?", [(ts, i) for i in ids])

    def crawl_frontier(self, limit: int, *, prefer_rating_max: int = 950, not_expanded_since: int | None = None) -> list[str]:
        """Active members worth expanding: never expanded first, then low-rated, then recently active."""
        cutoff = not_expanded_since if not_expanded_since is not None else now_ms()
        cur = self.conn.execute(
            """
            SELECT id FROM players
            WHERE active = 1 AND (expanded_at IS NULL OR expanded_at < ?)
            ORDER BY (expanded_at IS NOT NULL), (COALESCE(game_rating, 10000) > ?), (last_game_at IS NULL),
                     last_game_at DESC, id
            LIMIT ?
            """,
            (cutoff, prefer_rating_max, int(limit)),
        )
        return [row["id"] for row in cur]

    def most_active_members(self, limit: int) -> list[str]:
        cur = self.conn.execute(
            """
            SELECT id FROM players WHERE active = 1 AND last_game_at IS NOT NULL
            ORDER BY last_game_at DESC, COALESCE(games_recent, 0) DESC LIMIT ?
            """,
            (int(limit),),
        )
        return [row["id"] for row in cur]

    def members_to_recheck(self, older_than_ms: int, limit: int) -> list[str]:
        cur = self.conn.execute(
            "SELECT id FROM players WHERE active = 1 AND updated_at < ? ORDER BY updated_at ASC LIMIT ?",
            (older_than_ms, int(limit)),
        )
        return [row["id"] for row in cur]

    def expire_inactive_members(self, cutoff_ms: int, updated_at: int | None = None) -> int:
        """Deactivate members whose newest known game is older than the cutoff."""
        ts = updated_at or now_ms()
        with self.conn:
            cur = self.conn.execute(
                "UPDATE players SET active = 0, updated_at = ? WHERE active = 1 AND last_game_at IS NOT NULL AND last_game_at < ?",
                (ts, cutoff_ms),
            )
            return cur.rowcount if cur.rowcount >= 0 else 0

    def active_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM players WHERE active = 1").fetchone()[0]

    def activity_stats(self) -> dict:
        now = now_ms()
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS active,
                   SUM(last_game_at >= ?) AS last_day,
                   SUM(last_game_at >= ?) AS last_week,
                   SUM(last_game_at IS NOT NULL) AS with_games
            FROM players WHERE active = 1
            """,
            (now - DAY_MS, now - 7 * DAY_MS),
        ).fetchone()
        return {
            "active": row["active"] or 0,
            "last_day": row["last_day"] or 0,
            "last_week": row["last_week"] or 0,
            "with_games": row["with_games"] or 0,
        }

    def deactivate_players(self, ids: Iterable[str], updated_at: int | None = None) -> int:
        ts = updated_at or now_ms()
        ids = list(ids)
        if not ids:
            return 0
        with self.conn:
            cur = self.conn.executemany(
                "UPDATE players SET active = 0, updated_at = ? WHERE id = ? AND active = 1",
                [(ts, i) for i in ids],
            )
            return cur.rowcount if cur.rowcount >= 0 else 0

    def touch_players(self, ids: Iterable[str], updated_at: int | None = None) -> None:
        """Mark players as checked now without changing anything else."""
        ts = updated_at or now_ms()
        with self.conn:
            self.conn.executemany("UPDATE players SET updated_at = ? WHERE id = ?", [(ts, i) for i in ids])

    def active_player_ids(self) -> list[str]:
        cur = self.conn.execute("SELECT id FROM players WHERE active = 1 ORDER BY id")
        return [row["id"] for row in cur]

    def pool_last_updated(self) -> int | None:
        """Epoch ms of the newest player row, or None if the pool is empty."""
        return self.conn.execute("SELECT MAX(updated_at) FROM players").fetchone()[0]

    def player_ids(self) -> set[str]:
        return {row["id"] for row in self.conn.execute("SELECT id FROM players")}

    def active_players(self, limit: int | None = None) -> list[sqlite3.Row]:
        sql = "SELECT * FROM players WHERE active = 1 ORDER BY COALESCE(last_game_at, seen_at) DESC"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return list(self.conn.execute(sql))

    def pool_stats(self) -> dict:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(active) AS active,
                MIN(CASE WHEN active = 1 THEN blitz_rating END) AS min_rating,
                MAX(CASE WHEN active = 1 THEN blitz_rating END) AS max_rating,
                AVG(CASE WHEN active = 1 THEN blitz_rating END) AS avg_rating,
                MAX(updated_at) AS last_updated
            FROM players
            """
        ).fetchone()
        teams = self.conn.execute(
            "SELECT COUNT(*) AS n, SUM(last_scanned IS NOT NULL) AS scanned FROM teams"
        ).fetchone()
        return {
            "players_total": row["total"] or 0,
            "players_active": row["active"] or 0,
            "min_rating": row["min_rating"],
            "max_rating": row["max_rating"],
            "avg_rating": round(row["avg_rating"], 1) if row["avg_rating"] is not None else None,
            "last_updated": row["last_updated"],
            "teams_total": teams["n"] or 0,
            "teams_scanned": teams["scanned"] or 0,
        }

    def players_by_source(self) -> dict[str, int]:
        """Active player counts per source_team (teams and 'opponent')."""
        cur = self.conn.execute(
            "SELECT COALESCE(source_team, '?') AS src, COUNT(*) AS n FROM players WHERE active = 1 "
            "GROUP BY src ORDER BY n DESC"
        )
        return {row["src"]: row["n"] for row in cur}

    # -- discovered opponents -----------------------------------------------------------

    def in_pool(self, player_id: str, active_days: int = 7) -> bool:
        """True if the player is active, or was evaluated (and dropped) recently."""
        row = self.conn.execute("SELECT active, updated_at FROM players WHERE id = ?", (player_id,)).fetchone()
        if row is None:
            return False
        if row["active"]:
            return True
        return row["updated_at"] >= now_ms() - active_days * DAY_MS

    def record_discovered(self, player_id: str, rating: int, active_days: int = 7) -> None:
        """Remember an opponent to look up at the next pool refresh.

        A player fetched more than ``active_days`` ago becomes fetchable again.
        """
        ts = now_ms()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO discovered_players (id, first_seen, last_seen, last_rating, fetched_at)
                VALUES (?, ?, ?, ?, NULL)
                ON CONFLICT(id) DO UPDATE SET
                    last_seen = excluded.last_seen,
                    last_rating = excluded.last_rating,
                    fetched_at = CASE
                        WHEN discovered_players.fetched_at IS NOT NULL AND discovered_players.fetched_at < ?
                        THEN NULL ELSE discovered_players.fetched_at END
                """,
                (player_id, ts, ts, rating, ts - active_days * DAY_MS),
            )

    def discovered_to_fetch(self, limit: int | None = None) -> list[str]:
        sql = "SELECT id FROM discovered_players WHERE fetched_at IS NULL ORDER BY last_seen DESC"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return [row["id"] for row in self.conn.execute(sql)]

    def mark_discovered_fetched(self, ids: Iterable[str]) -> None:
        ts = now_ms()
        with self.conn:
            self.conn.executemany("UPDATE discovered_players SET fetched_at = ? WHERE id = ?", [(ts, i) for i in ids])

    def discovered_stats(self) -> dict:
        row = self.conn.execute(
            "SELECT COUNT(*) AS total, SUM(fetched_at IS NULL) AS pending FROM discovered_players"
        ).fetchone()
        return {"total": row["total"] or 0, "pending": row["pending"] or 0}

    # -- seen games (milestone 2) ----------------------------------------------------

    def game_seen(self, game_id: str) -> bool:
        return self.conn.execute("SELECT 1 FROM seen_games WHERE id = ?", (game_id,)).fetchone() is not None

    def record_game(self, game_id: str, accepted: bool) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO seen_games (id, first_seen, accepted) VALUES (?, ?, ?)",
                (game_id, now_ms(), int(accepted)),
            )
