import time

from backend.db import DAY_MS, Database, PlayerRow


def test_discovered_players_lifecycle(tmp_path):
    db = Database(tmp_path / "d.sqlite3")
    assert db.discovered_stats() == {"total": 0, "pending": 0}

    db.record_discovered("carol", 900)
    db.record_discovered("dave", 950)
    db.record_discovered("carol", 880)  # seen again: rating updated, still one row
    assert db.discovered_stats() == {"total": 2, "pending": 2}
    assert db.discovered_to_fetch() == ["carol", "dave"]  # most recently seen first
    assert db.discovered_to_fetch(limit=1) == ["carol"]

    db.mark_discovered_fetched(["carol"])
    assert db.discovered_to_fetch() == ["dave"]
    db.record_discovered("carol", 870)  # fetched recently: stays fetched
    assert db.discovered_to_fetch() == ["dave"]

    # A fetch older than active_days makes the player fetchable again.
    old = int(time.time() * 1000) - 8 * DAY_MS
    db.conn.execute("UPDATE discovered_players SET fetched_at = ? WHERE id = 'carol'", (old,))
    db.record_discovered("carol", 860, active_days=7)
    assert db.discovered_to_fetch() == ["carol", "dave"]
    db.close()


def test_activity_columns_and_crawl_helpers(tmp_path):
    db = Database(tmp_path / "d.sqlite3")
    now = int(time.time() * 1000)
    db.upsert_players([
        PlayerRow("old", "Old", 900, False, now, "games", last_game_at=now - 20 * DAY_MS, game_rating=900, games_recent=2),
        PlayerRow("fresh", "Fresh", 800, False, now, "games", last_game_at=now - DAY_MS, game_rating=800, games_recent=9),
        PlayerRow("team", "Team", 950, False, now, "some-team"),
    ])
    # newest game wins; an older sighting never lowers last_game_at or overwrites the rating
    db.upsert_players([PlayerRow("fresh", "Fresh", 820, False, now, None, last_game_at=now - 5 * DAY_MS, game_rating=760)])
    row = db.conn.execute("SELECT last_game_at, game_rating, games_recent, source_team FROM players WHERE id='fresh'").fetchone()
    assert tuple(row) == (now - DAY_MS, 800, 9, "games")
    db.update_player_activity("fresh", last_game_at=now, game_rating=790, games_recent=10, expanded_at=now)
    row = db.conn.execute("SELECT last_game_at, game_rating, games_recent, expanded_at FROM players WHERE id='fresh'").fetchone()
    assert tuple(row) == (now, 790, 10, now)
    # never expanded first (low-rated/recent first among those); expanded members come last
    assert db.crawl_frontier(10) == ["old", "team", "fresh"]
    assert db.crawl_frontier(10, not_expanded_since=now - 1000) == ["old", "team"]
    assert db.most_active_members(5) == ["fresh", "old"]
    assert db.activity_stats() == {"active": 3, "last_day": 1, "last_week": 1, "with_games": 2}
    assert db.expire_inactive_members(now - 14 * DAY_MS) == 1
    assert sorted(db.active_player_ids()) == ["fresh", "team"]
    assert db.active_count() == 2
    db.touch_players(["team"], updated_at=now - 10 * DAY_MS)
    assert db.members_to_recheck(now - DAY_MS, limit=10) == ["team"]
    db.close()


def test_schema_migration_adds_columns_to_old_databases(tmp_path):
    import sqlite3

    path = tmp_path / "old.sqlite3"
    conn = sqlite3.connect(path)
    conn.executescript(
        "CREATE TABLE players (id TEXT PRIMARY KEY, username TEXT, blitz_rating INTEGER, provisional INTEGER NOT NULL DEFAULT 0,"
        " seen_at INTEGER, active INTEGER NOT NULL DEFAULT 1, updated_at INTEGER NOT NULL, source_team TEXT);"
        "INSERT INTO players (id, username, blitz_rating, updated_at) VALUES ('a', 'A', 900, 1);"
    )
    conn.commit()
    conn.close()
    db = Database(path)
    assert db.active_player_ids() == ["a"]
    assert db.activity_stats()["with_games"] == 0
    db.close()


def test_in_pool(tmp_path):
    db = Database(tmp_path / "d.sqlite3")
    now = int(time.time() * 1000)
    db.upsert_players([PlayerRow("alice", "Alice", 800, False, now), PlayerRow("bob", "Bob", 800, False, now)])
    db.deactivate_players(["bob"])
    assert db.in_pool("alice")
    assert db.in_pool("bob")  # dropped, but evaluated recently
    db.conn.execute("UPDATE players SET updated_at = ? WHERE id = 'bob'", (now - 8 * DAY_MS,))
    assert not db.in_pool("bob", active_days=7)
    assert not db.in_pool("nobody")
    assert db.players_by_source() == {"?": 1}
    db.close()
