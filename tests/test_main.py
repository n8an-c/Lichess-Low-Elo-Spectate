import asyncio

import httpx

from backend.config import DetectionSettings, PoolSettings, Settings, StorageSettings
from backend.lichess import LichessClient, LichessSettings
from backend.main import create_app


def make_app(tmp_path):
    settings = Settings(
        pool=PoolSettings(rating_cap=1000),
        detection=DetectionSettings(poll_interval_seconds=0.05),
        storage=StorageSettings(sqlite_path=str(tmp_path / "s.sqlite3")),
    )
    client = LichessClient(
        LichessSettings(courtesy_delay_seconds=0),
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[])),
    )
    return create_app(settings, skip_pool_build=True, client=client)


async def test_state_next_and_events(tmp_path, monkeypatch):
    import backend.main as main

    monkeypatch.setattr(main, "PING_SECONDS", 0.05)  # the SSE generator notices the closed client at its next ping
    app = make_app(tmp_path)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
            r = await http.get("/api/state")
            assert r.status_code == 200
            assert r.json()["status"] == "waiting"
            assert r.json()["waiting"] == {"candidates": 0, "poolSize": 0}

            r = await http.post("/api/next")
            assert r.status_code == 200
            assert r.json()["status"] == "waiting"

            r = await http.get("/api/candidates")
            assert r.json() == []

            r = await http.post("/api/watch/short")
            assert r.status_code == 400

            async with http.stream("GET", "/api/events") as resp:
                assert resp.headers["content-type"].startswith("text/event-stream")
                first = await asyncio.wait_for(anext(resp.aiter_lines()), 2)
                assert first == "event: state"


async def test_root_without_frontend_build_explains(tmp_path, monkeypatch):
    import backend.main as main

    monkeypatch.setattr(main, "FRONTEND_DIST", tmp_path / "nope")
    app = make_app(tmp_path)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
            r = await http.get("/")
            assert r.status_code == 200
            assert "npm run build" in r.text
