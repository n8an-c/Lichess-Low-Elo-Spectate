import asyncio

import httpx

from backend.config import DetectionSettings, PoolSettings, Settings, StorageSettings
from backend.lichess import LichessClient, LichessSettings
from backend.service import Spectator


def make_settings(tmp_path, mode):
    return Settings(
        pool=PoolSettings(rating_cap=1000),
        detection=DetectionSettings(mode=mode, poll_interval_seconds=0.01),
        storage=StorageSettings(sqlite_path=str(tmp_path / f"{mode}.sqlite3")),
    )


def quiet_client():
    return LichessClient(
        LichessSettings(courtesy_delay_seconds=0),
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[])),
    )


async def test_detection_mode_selects_loops(tmp_path):
    for mode, expected in [
        ("poll", ["pool", "poller", "verifier", "expiry"]),
        ("stream", ["pool", "gamestream", "verifier", "expiry"]),
        ("both", ["pool", "poller", "gamestream", "verifier", "expiry"]),
    ]:
        spectator = Spectator(make_settings(tmp_path, mode), skip_pool_build=True, client=quiet_client())
        assert list(spectator.loops()) == expected
        if spectator.poller is not None:
            assert spectator.poller._pending is spectator.pending
        if spectator.watcher is not None:
            assert spectator.watcher.pending is spectator.pending
        await spectator.client.aclose()
        spectator.db.close()


def test_invalid_mode_is_rejected():
    import pytest

    with pytest.raises(ValueError):
        DetectionSettings(mode="push")


async def test_discover_records_only_new_sub_cap_players(tmp_path):
    spectator = Spectator(make_settings(tmp_path, "poll"), skip_pool_build=True, client=quiet_client())
    from backend.db import PlayerRow

    spectator.db.upsert_players([PlayerRow("alice", "Alice", 800, False, 0)])
    spectator.discover("alice", 820)  # already pooled
    spectator.discover("strong", 1400)  # above the cap
    spectator.discover("newbie", 760)
    assert spectator.db.discovered_to_fetch() == ["newbie"]
    assert spectator.discovered_total == 1
    await spectator.client.aclose()
    spectator.db.close()


async def test_crashed_loop_is_restarted_and_db_stays_open(tmp_path):
    settings = Settings(
        pool=PoolSettings(rating_cap=1000),
        detection=DetectionSettings(poll_interval_seconds=0.01),
        storage=StorageSettings(sqlite_path=str(tmp_path / "s.sqlite3")),
    )
    client = LichessClient(
        LichessSettings(courtesy_delay_seconds=0),
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[])),
    )
    spectator = Spectator(settings, skip_pool_build=True, client=client)
    spectator.restart_delay = 0.01
    runs = 0

    async def crashing_poller():
        nonlocal runs
        runs += 1
        raise RuntimeError("simulated bug in the poller")

    spectator.poller.run = crashing_poller  # type: ignore[method-assign]
    task = asyncio.create_task(spectator.run())
    await asyncio.sleep(0.2)
    assert runs >= 3  # restarted after each crash
    assert spectator.crashes == runs
    # Other loops and the database are unaffected.
    assert spectator.db.active_player_ids() == []
    assert spectator.db.pool_last_updated() is None
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
