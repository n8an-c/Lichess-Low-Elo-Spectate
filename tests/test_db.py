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
