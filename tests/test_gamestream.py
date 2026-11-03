import asyncio
import json

import httpx
import pytest

from backend.config import DetectionSettings, PoolSettings, Settings
from backend.db import Database, PlayerRow
from backend.gamestream import GameStreamWatcher, parse_time_control
from backend.lichess import LichessClient, LichessSettings

SETTINGS = Settings(
    pool=PoolSettings(rating_cap=1000),
    detection=DetectionSettings(time_control="3+0", stream_backoff_seconds=0.01, stream_backoff_max_seconds=0.02),
)


def doc(game_id="game0001", **over):
    base = {
        "id": game_id,
        "rated": True,
        "variant": "standard",
        "speed": "blitz",
        "perf": "blitz",
        "createdAt": 1_800_000_000_000,
        "status": 20,
        "statusName": "started",
        "players": {"white": {"userId": "alice", "rating": 812}, "black": {"userId": "bob", "rating": 947}},
        "clock": {"initial": 180, "increment": 0},
    }
    base.update(over)
    return base


def make_watcher(tmp_path, transport=None, discover=None):
    db = Database(tmp_path / "g.sqlite3")
    client = LichessClient(
        LichessSettings(courtesy_delay_seconds=0),
        transport=transport or httpx.MockTransport(lambda r: httpx.Response(200, text="")),
    )
    queue: asyncio.Queue = asyncio.Queue()
    pending: set[str] = set()
    event = asyncio.Event()
    watcher = GameStreamWatcher(client, db, SETTINGS, queue, pending, event, discover)
    return watcher, db, client, queue, pending, event


def test_parse_time_control():
    assert parse_time_control("3+0") == (180, 0)
    assert parse_time_control("5+3") == (300, 3)
    assert parse_time_control("nope") is None


async def test_handle_queues_and_dedupes(tmp_path):
    watcher, db, client, queue, pending, _ = make_watcher(tmp_path)
    assert watcher.handle(doc()) == "game0001"
    assert queue.get_nowait() == "game0001"
    assert "game0001" in pending
    assert watcher.handle(doc()) is None  # pending
    pending.clear()
    db.record_game("game0001", accepted=False)
    assert watcher.handle(doc()) is None  # seen before
    assert watcher.queued == 1
    await client.aclose()
    db.close()


@pytest.mark.parametrize(
    "over",
    [
        {"statusName": "mate"},
        {"statusName": "aborted"},
        {"rated": False},
        {"variant": "chess960"},
        {"clock": {"initial": 180, "increment": 2}},
        {"clock": {"initial": 300, "increment": 0}},
        {"players": {"white": {"userId": "alice", "rating": 1000}, "black": {"userId": "bob", "rating": 900}}},
        {"players": {"white": {"userId": "alice", "rating": 900}, "black": {"aiLevel": 1}}},
        {"id": None},
    ],
    ids=["finished", "aborted", "unrated", "chess960", "3+2", "5+0", "rating-at-cap", "ai-opponent", "no-id"],
)
async def test_handle_rejects(tmp_path, over):
    watcher, db, client, queue, _, _ = make_watcher(tmp_path)
    assert watcher.handle(doc(**over)) is None
    assert queue.empty()
    await client.aclose()
    db.close()


async def test_handle_discovers_both_players_even_when_game_is_rejected(tmp_path):
    seen = []
    watcher, db, client, queue, _, _ = make_watcher(tmp_path, discover=lambda uid, rating: seen.append((uid, rating)))
    strong = {"white": {"userId": "alice", "rating": 2400}, "black": {"userId": "bob", "rating": 950}}
    assert watcher.handle(doc(players=strong)) is None  # alice too strong -> game rejected
    assert seen == [("alice", 2400), ("bob", 950)]  # ...but both players reported
    seen.clear()
    watcher.handle(doc(variant="chess960"))  # not a blitz rating: nothing to learn
    assert seen == []
    await client.aclose()
    db.close()


def ndjson(objs):
    return "".join(json.dumps(o) + "\n" for o in objs)


class RecordingTransport:
    """Records games-by-users requests and serves scripted stream bodies."""

    def __init__(self, bodies_for=None, keep_open=False):
        self.requests = []
        self.bodies_for = bodies_for or (lambda ids: [])
        self.keep_open = keep_open

    def transport(self):
        async def body(ids):
            yield ndjson(self.bodies_for(ids)).encode()
            if self.keep_open:
                await asyncio.sleep(10)

        def handler(request):
            assert request.url.path == "/api/stream/games-by-users"
            assert request.method == "POST"
            assert request.headers["content-type"] == "text/plain"
            assert request.url.params["withCurrentGames"] == "true"
            ids = request.content.decode().split(",")
            self.requests.append(ids)
            return httpx.Response(200, content=body(ids))

        return httpx.MockTransport(handler)


async def test_run_opens_one_stream_per_300_players_and_queues_games(tmp_path):
    rec = RecordingTransport(bodies_for=lambda ids: [doc(f"g-{ids[0]}")], keep_open=True)
    watcher, db, client, queue, _, _ = make_watcher(tmp_path, transport=rec.transport())
    db.upsert_players([PlayerRow(f"u{i:03d}", f"U{i}", 800, False, 0) for i in range(400)])
    task = asyncio.create_task(watcher.run())
    first = await asyncio.wait_for(queue.get(), 2)
    second = await asyncio.wait_for(queue.get(), 2)
    assert {first, second} == {"g-u000", "g-u300"}
    assert sorted(len(r) for r in rec.requests) == [100, 300]
    assert watcher.streams == 2
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await client.aclose()
    db.close()


async def test_run_reconnects_when_a_stream_ends(tmp_path):
    rec = RecordingTransport(bodies_for=lambda ids: [], keep_open=False)
    watcher, db, client, queue, _, _ = make_watcher(tmp_path, transport=rec.transport())
    db.upsert_players([PlayerRow("u1", "U1", 800, False, 0)])
    task = asyncio.create_task(watcher.run())
    await asyncio.sleep(0.15)
    assert len(rec.requests) >= 3  # ended streams are reopened with (tiny) backoff
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await client.aclose()
    db.close()


async def test_run_rebuilds_streams_after_pool_refresh(tmp_path):
    rec = RecordingTransport(bodies_for=lambda ids: [], keep_open=True)
    watcher, db, client, queue, _, event = make_watcher(tmp_path, transport=rec.transport())
    db.upsert_players([PlayerRow("u1", "U1", 800, False, 0)])
    task = asyncio.create_task(watcher.run())
    await asyncio.sleep(0.05)
    assert rec.requests == [["u1"]]
    db.upsert_players([PlayerRow("u2", "U2", 850, False, 0)])
    event.set()
    await asyncio.sleep(0.05)
    assert rec.requests[-1] == ["u1", "u2"]
    assert not event.is_set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    await client.aclose()
    db.close()
