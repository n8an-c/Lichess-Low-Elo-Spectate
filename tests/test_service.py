import asyncio

import httpx

from backend.config import DetectionSettings, PoolSettings, Settings, StorageSettings
from backend.lichess import LichessClient, LichessSettings
from backend.service import Spectator


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
