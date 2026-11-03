import asyncio

import httpx

from backend.config import DetectionSettings
from backend.db import Database
from backend.lichess import LichessClient, LichessSettings
from backend.poller import Poller


def make_poller(tmp_path):
    db = Database(tmp_path / "p.sqlite3")
    client = LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[])))
    return Poller(client, db, DetectionSettings(time_control="3+0"), asyncio.Queue()), db


def test_detect_filters_and_dedupes(tmp_path):
    poller, db = make_poller(tmp_path)
    db.record_game("seenbefore", accepted=False)
    statuses = [
        {"id": "a", "name": "A", "online": True},
        {"id": "b", "name": "B", "online": True, "playing": {"id": "game30", "clock": "3+0"}},
        {"id": "c", "name": "C", "online": True, "playing": {"id": "game30", "clock": "3+0"}},  # opponent, same game
        {"id": "d", "name": "D", "online": True, "playing": {"id": "game32", "clock": "3+2"}},
        {"id": "e", "name": "E", "online": True, "playing": {"id": "game960", "clock": "3+0", "variant": "chess960"}},
        {"id": "f", "name": "F", "online": True, "playing": {"id": "seenbefore", "clock": "3+0"}},
        {"id": "g", "name": "G", "online": True, "playing": True},  # withGameIds-style payload, no metas
    ]
    assert poller.detect(statuses) == ["game30"]
    assert poller.last_pass_playing == 5
    assert poller.last_pass_matching == 3  # game30 twice + seenbefore
    # Still pending: not re-queued on the next pass.
    assert poller.detect(statuses[1:2]) == []
    poller.release("game30")
    assert poller.detect(statuses[1:2]) == ["game30"]
    db.close()


async def test_run_survives_network_errors(tmp_path):
    db = Database(tmp_path / "p.sqlite3")
    from backend.db import PlayerRow

    db.upsert_players([PlayerRow("u1", "U1", 800, False, 0)])
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(200, json=[{"id": "u1", "name": "U1", "playing": {"id": "g-after", "clock": "3+0"}}])

    client = LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=httpx.MockTransport(handler))
    queue: asyncio.Queue = asyncio.Queue()
    poller = Poller(client, db, DetectionSettings(poll_interval_seconds=0), queue)
    task = asyncio.create_task(poller.run())
    assert await asyncio.wait_for(queue.get(), 2) == "g-after"
    task.cancel()
    assert calls >= 2
    await client.aclose()
    db.close()


async def test_run_queues_games_and_batches_by_100(tmp_path):
    db = Database(tmp_path / "p.sqlite3")
    from backend.db import PlayerRow

    db.upsert_players([PlayerRow(f"u{i:03d}", f"U{i}", 800, False, 0) for i in range(150)])
    calls = []

    def handler(request):
        ids = request.url.params["ids"].split(",")
        calls.append(len(ids))
        assert request.url.params["withGameMetas"] == "true"
        first = ids[0]
        return httpx.Response(200, json=[{"id": first, "name": first, "playing": {"id": f"g-{first}", "clock": "3+0"}}])

    client = LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=httpx.MockTransport(handler))
    queue: asyncio.Queue = asyncio.Queue()
    poller = Poller(client, db, DetectionSettings(poll_interval_seconds=0), queue)
    task = asyncio.create_task(poller.run())
    first = await asyncio.wait_for(queue.get(), 2)
    second = await asyncio.wait_for(queue.get(), 2)
    task.cancel()
    assert {first, second} == {"g-u000", "g-u100"}
    assert calls[:2] == [100, 50]
    await client.aclose()
    db.close()
