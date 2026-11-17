import json

import httpx
import pytest

from backend.config import PoolSettings
from backend.db import Database
from backend.lichess import LichessClient, LichessSettings
from backend.pool import build_pool, qualifies

NOW = 1_700_000_000_000
DAY = 86_400_000
SETTINGS = PoolSettings(rating_cap=1000, active_days=7)


def user(**overrides):
    base = {
        "id": "alice",
        "username": "Alice",
        "perfs": {"blitz": {"games": 50, "rating": 900, "rd": 60, "prog": 0}},
        "seenAt": NOW - DAY,
    }
    base.update(overrides)
    return base


def test_qualifies_happy_path():
    assert qualifies(user(), SETTINGS, NOW)


@pytest.mark.parametrize(
    "overrides",
    [
        {"perfs": {}},
        {"perfs": {"bullet": {"games": 5, "rating": 800, "rd": 60, "prog": 0}}},
        {"perfs": {"blitz": {"games": 2, "rating": 800, "rd": 300, "prog": 0, "prov": True}}},
        {"perfs": {"blitz": {"games": 50, "rating": 1000, "rd": 60, "prog": 0}}},
        {"seenAt": NOW - 8 * DAY},
        {"seenAt": None},
        {"title": "BOT"},
        {"disabled": True},
        {"tosViolation": True},
    ],
    ids=["no-perfs", "no-blitz", "provisional", "rating-at-cap", "stale", "never-seen", "bot", "disabled", "tos"],
)
def test_qualifies_rejects(overrides):
    assert not qualifies(user(**overrides), SETTINGS, NOW)


def ndjson(objs):
    return "".join(json.dumps(o) + "\n" for o in objs)


def fake_lichess(members_by_team, search_results=None, profiles=None):
    search_results = search_results or {}
    profiles = profiles or {}
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        path = request.url.path
        if path == "/api/team/search":
            text = request.url.params["text"]
            return httpx.Response(
                200,
                json={
                    "currentPage": 1,
                    "maxPerPage": 15,
                    "currentPageResults": search_results.get(text, []),
                    "previousPage": None,
                    "nextPage": None,
                    "nbResults": len(search_results.get(text, [])),
                    "nbPages": 1,
                },
            )
        if path.startswith("/api/team/") and path.endswith("/users"):
            team_id = path.split("/")[3]
            if team_id not in members_by_team:
                return httpx.Response(404, json={"error": "not found"})
            return httpx.Response(200, text=ndjson(members_by_team[team_id]))
        if path == "/api/users" and request.method == "POST":
            ids = request.content.decode().split(",")
            return httpx.Response(200, json=[profiles[i] for i in ids if i in profiles])
        return httpx.Response(500, text=f"unexpected {request.method} {path}")

    return httpx.MockTransport(handler), calls


async def test_build_pool_end_to_end(tmp_path):
    members = {
        "beginners": [
            user(id="alice", username="Alice"),
            user(id="bob", username="Bob", perfs={"blitz": {"games": 1, "rating": 700, "rd": 300, "prog": 0, "prov": True}}),
            user(id="carol", username="Carol", perfs={"blitz": {"games": 90, "rating": 1450, "rd": 50, "prog": 0}}),
        ],
        "found-by-keyword": [user(id="dave", username="Dave", perfs={"blitz": {"games": 30, "rating": 999, "rd": 70, "prog": 0}})],
    }
    search = {"beginner": [{"id": "found-by-keyword", "name": "Found", "nbMembers": 1}]}
    # "erin" was pooled earlier, is no longer in any scanned team, and now rates too high.
    # "frank" was pooled earlier and still qualifies.
    profiles = {
        "erin": user(id="erin", username="Erin", perfs={"blitz": {"games": 200, "rating": 1100, "rd": 45, "prog": 0}}),
        "frank": user(id="frank", username="Frank", perfs={"blitz": {"games": 200, "rating": 850, "rd": 45, "prog": 0}}),
    }
    transport, calls = fake_lichess(members, search, profiles)
    settings = PoolSettings(team_ids=["beginners", "missing-team"], team_keywords=["beginner"], rating_cap=1000, active_days=7)

    db = Database(tmp_path / "t.sqlite3")
    from backend.db import PlayerRow

    db.upsert_players(
        [
            PlayerRow("erin", "Erin", 950, False, NOW),
            PlayerRow("frank", "Frank", 850, False, NOW),
        ]
    )

    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        result = await build_pool(client, db, settings, now_ms=NOW)

    assert result.teams_found == 3
    assert result.teams_scanned == 2
    assert result.teams_skipped == 1
    assert sorted(db.active_player_ids()) == ["alice", "dave", "frank"]
    inactive = {r["id"] for r in db.conn.execute("SELECT id FROM players WHERE active = 0")}
    assert inactive == {"erin"}  # bob/carol never qualified, so they were never stored
    assert db.conn.execute("SELECT source_team FROM players WHERE id='dave'").fetchone()[0] == "found-by-keyword"
    assert result.refreshed == 2
    assert result.added == 2  # alice, dave
    assert result.deactivated == 1  # erin
    assert any(c.method == "POST" and c.url.path == "/api/users" for c in calls)
    db.close()


async def test_discovered_opponents_are_fetched_with_the_remaining_budget(tmp_path):
    profiles = {
        "stale1": user(id="stale1", username="Stale1", perfs={"blitz": {"games": 50, "rating": 850, "rd": 60, "prog": 0}}),
        "opp-good": user(id="opp-good", username="OppGood", perfs={"blitz": {"games": 40, "rating": 700, "rd": 60, "prog": 0}}),
        "opp-prov": user(id="opp-prov", username="OppProv", perfs={"blitz": {"games": 3, "rating": 700, "rd": 300, "prog": 0, "prov": True}}),
        "opp-strong": user(id="opp-strong", username="OppStrong", perfs={"blitz": {"games": 300, "rating": 1300, "rd": 45, "prog": 0}}),
    }
    transport, calls = fake_lichess({}, {}, profiles)
    db = Database(tmp_path / "t.sqlite3")
    from backend.db import PlayerRow

    db.upsert_players([PlayerRow("stale1", "Stale1", 850, False, NOW)])
    for pid, rating in [("opp-good", 720), ("opp-prov", 700), ("opp-strong", 990)]:
        db.record_discovered(pid, rating)

    # Budget of one batch: the stale pool refresh uses it, opponents wait.
    settings = PoolSettings(max_profile_refresh_batches=1)
    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        result = await build_pool(client, db, settings, now_ms=NOW)
    assert result.refreshed == 1
    assert result.discovered_fetched == 0 and result.discovered_pending == 3
    assert sorted(db.discovered_to_fetch()) == ["opp-good", "opp-prov", "opp-strong"]

    # Budget of two: the second batch looks up the opponents.
    settings = PoolSettings(max_profile_refresh_batches=2)
    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        result = await build_pool(client, db, settings, now_ms=NOW)
    assert result.discovered_fetched == 3 and result.discovered_added == 1 and result.discovered_pending == 0
    assert sorted(db.active_player_ids()) == ["opp-good", "stale1"]
    assert db.conn.execute("SELECT source_team FROM players WHERE id = 'opp-good'").fetchone()[0] == "opponent"
    assert db.discovered_to_fetch() == []  # all marked fetched, qualifying or not
    posts = [c.content.decode() for c in calls if c.method == "POST" and c.url.path == "/api/users"]
    assert set(posts[-1].split(",")) == {"opp-strong", "opp-prov", "opp-good"}
    assert "opponents: 3 fetched, 1 added, 0 waiting" in result.summary()
    db.close()


async def test_max_members_per_team_stops_stream_and_releases_lock(tmp_path):
    members = {"big": [user(id=f"u{i}", username=f"U{i}") for i in range(50)]}
    transport, calls = fake_lichess(members)
    settings = PoolSettings(team_ids=["big"], max_members_per_team=10)
    db = Database(tmp_path / "t.sqlite3")
    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        result = await build_pool(client, db, settings, now_ms=NOW)
        # The lock must be free again after the early-stopped stream.
        assert await client.search_teams("x") is not None
    assert result.users_seen == 10
    assert len(db.active_player_ids()) == 10
    db.close()


async def test_max_teams_limits_scan(tmp_path):
    members = {"a": [user(id="a1")], "b": [user(id="b1")], "c": [user(id="c1")]}
    transport, _ = fake_lichess(members)
    db = Database(tmp_path / "t.sqlite3")
    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        result = await build_pool(client, db, PoolSettings(team_ids=["a", "b", "c"]), now_ms=NOW, max_teams=2)
    assert result.teams_found == 3
    assert result.teams_scanned == 2
    assert sorted(db.active_player_ids()) == ["a1", "b1"]
    db.close()
