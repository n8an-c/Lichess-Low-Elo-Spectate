import httpx

from backend.config import DetectionSettings, PoolSettings, Settings, StorageSettings
from backend.lichess import LichessClient, LichessSettings
from backend.main import create_app
from backend.verifier import GameInfo, PlayerInfo


def make_app(tmp_path):
    settings = Settings(
        pool=PoolSettings(rating_cap=1000),
        detection=DetectionSettings(mode="poll", poll_interval_seconds=0.05),
        storage=StorageSettings(sqlite_path=str(tmp_path / "s.sqlite3")),
    )
    client = LichessClient(
        LichessSettings(courtesy_delay_seconds=0),
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[])),
    )
    return create_app(settings, skip_pool_build=True, client=client)


async def test_games_feed_and_index(tmp_path):
    app = make_app(tmp_path)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
            r = await http.get("/api/games")
            assert r.status_code == 200
            body = r.json()
            assert body["games"] == []
            assert body["stats"]["poolSize"] == 0 and body["stats"]["mode"] == "poll"
            assert body["stats"]["accepted"] == 0

            spectator = app.state.spectator
            spectator.record_accepted(
                GameInfo("abcd1234", PlayerInfo("alice", "Alice", 812), PlayerInfo("bob", "Bob", 947), 1_800_000_000_000, 4)
            )
            spectator.record_accepted(
                GameInfo("efgh5678", PlayerInfo("carol", "Carol", 700), PlayerInfo("dave", "Dave", 650), 1_800_000_100_000, 2)
            )
            r = await http.get("/api/games?limit=1")
            games = r.json()["games"]
            assert [g["id"] for g in games] == ["efgh5678"]  # newest first, limit honoured
            assert games[0]["url"] == "https://lichess.org/efgh5678"
            assert games[0]["white"]["name"] == "Carol" and games[0]["pliesAtJoin"] == 2
            assert r.json()["stats"]["accepted"] == 2

            r = await http.get("/api/candidates")
            assert {g["id"] for g in r.json()} == {"abcd1234", "efgh5678"}

            r = await http.get("/")
            assert r.status_code == 200
            assert "Watch on Lichess" in r.text and "/api/games" in r.text


async def test_root_without_frontend_file_explains(tmp_path, monkeypatch):
    import backend.main as main

    monkeypatch.setattr(main, "FRONTEND_INDEX", tmp_path / "nope.html")
    app = make_app(tmp_path)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
            r = await http.get("/")
            assert r.status_code == 200 and "not found" in r.text
