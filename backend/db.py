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
"""


@dataclass(frozen=True)
class PlayerRow:
    id: str
    username: str
    blitz_rating: int
    provisional: bool
    seen_at: int  # epoch milliseconds, as Lichess reports it
    source_team: str | None = None


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
                    (id, username, blitz_rating, provisional, seen_at, active, updated_at, source_team)
                VALUES (:id, :username, :blitz_rating, :provisional, :seen_at, 1, :updated_at, :source_team)
                ON CONFLICT(id) DO UPDATE SET
                    username = excluded.username,
                    blitz_rating = excluded.blitz_rating,
                    provisional = excluded.provisional,
                    seen_at = excluded.seen_at,
                    active = 1,
                    updated_at = excluded.updated_at,
                    source_team = COALESCE(excluded.source_team, players.source_team)
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
                    }
                    for r in rows
                ],
            )

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

    def active_player_ids(self) -> list[str]:
        cur = self.conn.execute("SELECT id FROM players WHERE active = 1 ORDER BY id")
        return [row["id"] for row in cur]

    def pool_last_updated(self) -> int | None:
        """Epoch ms of the newest player row, or None if the pool is empty."""
        return self.conn.execute("SELECT MAX(updated_at) FROM players").fetchone()[0]

    def player_ids(self) -> set[str]:
        return {row["id"] for row in self.conn.execute("SELECT id FROM players")}

    def active_players(self, limit: int | None = None) -> list[sqlite3.Row]:
        sql = "SELECT * FROM players WHERE active = 1 ORDER BY seen_at DESC"
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

    # -- seen games (milestone 2) ----------------------------------------------------

    def game_seen(self, game_id: str) -> bool:
        return self.conn.execute("SELECT 1 FROM seen_games WHERE id = ?", (game_id,)).fetchone() is not None

    def record_game(self, game_id: str, accepted: bool) -> None:
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO seen_games (id, first_seen, accepted) VALUES (?, ?, ?)",
                (game_id, now_ms(), int(accepted)),
            )
