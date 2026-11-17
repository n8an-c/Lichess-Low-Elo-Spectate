import asyncio

import httpx

from backend.config import DetectionSettings
from backend.db import Database, PlayerRow
from backend.lichess import LichessClient, LichessSettings
from backend.poller import Poller


def make_poller(tmp_path, handler=None, **det):
    db = Database(tmp_path / "p.sqlite3")
    handler = handler or (lambda r: httpx.Response(200, json=[]))
    client = LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=httpx.MockTransport(handler))
    settings = DetectionSettings(time_controls=["3+0"], hot_poll_seconds=0, poll_interval_seconds=0, **det)
    return Poller(client, db, settings, asyncio.Queue()), db, client


def test_detect_filters_dedupes_and_tracks_hot_set(tmp_path):
    poller, db, _ = make_poller(tmp_path)
    db.record_game("seenbefore", accepted=False)
    statuses = [
        {"id": "a", "name": "A", "online": True},
        {"id": "b", "name": "B", "online": True, "playing": {"id": "game30", "clock": "3+0"}},
        {"id": "c", "name": "C", "online": True, "playing": {"id": "game30", "clock": "3+0"}},  # opponent, same game
        {"id": "d", "name": "D", "online": True, "playing": {"id": "game32", "clock": "3+2"}},
        {"id": "e", "name": "E", "online": True, "playing": {"id": "game960", "clock": "3+0", "variant": "chess960"}},
        {"id": "f", "name": "F", "online": True, "playing": {"id": "seenbefore", "clock": "3+0"}},
        {"id": "g", "name": "G", "online": True, "playing": True},  # withGameIds-style payload, no metas
        {"id": "h", "name": "H"},  # offline
    ]
    assert poller.detect(statuses, now=100.0) == ["game30"]
    assert set(poller.hot) == {"a", "b", "c", "d", "e", "f", "g"}
    assert poller.hot["a"] == 100.0
    # Still pending: not re-queued on the next pass; and an offline report drops a player from the hot set.
    assert poller.detect([{"id": "a", "name": "A", "online": False}, statuses[1]], now=101.0) == []
    assert "a" not in poller.hot
    poller.release("game30")
    assert poller.detect(statuses[1:2]) == ["game30"]
    db.close()


def test_detect_accepts_any_configured_time_control(tmp_path):
    poller, db, _ = make_poller(tmp_path)
    poller.clocks = {"3+0", "5+0"}
    statuses = [
        {"id": "b", "online": True, "playing": {"id": "g50", "clock": "5+0"}},
        {"id": "c", "online": True, "playing": {"id": "g53", "clock": "5+3"}},
    ]
    assert poller.detect(statuses) == ["g50"]
    db.close()


async def test_run_polls_hot_players_every_tick_and_sweeps_cold_ones(tmp_path):
    calls: list[list[str]] = []

    def handler(request):
        ids = request.url.params["ids"].split(",")
        calls.append(ids)
        assert request.url.params["withGameMetas"] == "true"
        out = []
        for pid in ids:
            entry = {"id": pid, "name": pid}
            if pid == "u005":  # the one player who is online; starts a 3+0 game once polled hot
                entry["online"] = True
                if any("u005" in c and len(c) == 1 for c in calls[:-1]):
                    entry["playing"] = {"id": "g-u005", "clock": "3+0"}
            out.append(entry)
        return httpx.Response(200, json=out)

    poller, db, client = make_poller(tmp_path, handler, cold_batches_per_tick=1)
    db.upsert_players([PlayerRow(f"u{i:03d}", f"U{i}", 800, False, 0) for i in range(150)])
    task = asyncio.create_task(poller.run())
    game = await asyncio.wait_for(poller.queue.get(), 3)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert game == "g-u005"
    # First tick: no hot players yet, one cold batch of 100. Then u005 is hot and polled alone every tick.
    assert len(calls[0]) == 100
    assert ["u005"] in calls
    assert poller.pool_size == 150
    assert poller.requests >= 3
    await client.aclose()
    db.close()


async def test_run_completes_cold_sweeps_and_counts_passes(tmp_path):
    def handler(request):
        ids = request.url.params["ids"].split(",")
        return httpx.Response(200, json=[{"id": pid, "name": pid} for pid in ids])

    poller, db, client = make_poller(tmp_path, handler, cold_batches_per_tick=2)
    db.upsert_players([PlayerRow(f"u{i:03d}", f"U{i}", 800, False, 0) for i in range(250)])
    task = asyncio.create_task(poller.run())

    async def until_passes(n):
        while poller.passes < n:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(until_passes(2), 3)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert poller.passes >= 2
    assert poller.last_pass_online == 0 and poller.last_pass_playing == 0
    assert poller.requests >= 6  # 250 players = 3 batches per sweep
    await client.aclose()
    db.close()


async def test_run_survives_network_errors(tmp_path):
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(200, json=[{"id": "u1", "name": "U1", "online": True, "playing": {"id": "g-after", "clock": "3+0"}}])

    poller, db, client = make_poller(tmp_path, handler)
    db.upsert_players([PlayerRow("u1", "U1", 800, False, 0)])
    task = asyncio.create_task(poller.run())
    assert await asyncio.wait_for(poller.queue.get(), 2) == "g-after"
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert calls >= 2
    await client.aclose()
    db.close()
