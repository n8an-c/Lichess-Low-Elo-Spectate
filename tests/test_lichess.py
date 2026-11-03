import asyncio
import json

import httpx
import pytest

from backend.lichess import (
    LichessClient,
    LichessError,
    LichessHTTPError,
    LichessRateLimited,
    LichessSettings,
    LichessTransportError,
)


def make_client(handler, pause=0.2, delay=0.0):
    settings = LichessSettings(courtesy_delay_seconds=delay, rate_limit_pause_seconds=pause)
    return LichessClient(settings, transport=httpx.MockTransport(handler))


async def test_429_pauses_then_retries_once():
    statuses = iter([429, 200])
    seen = []

    def handler(request):
        seen.append(asyncio.get_event_loop().time())
        code = next(statuses)
        return httpx.Response(code, json={"ok": True} if code == 200 else {"error": "slow down"})

    async with make_client(handler, pause=0.2) as client:
        assert await client.get_json("/api/x") == {"ok": True}
        assert client.rate_limit_hits == 1
    assert len(seen) == 2
    assert seen[1] - seen[0] >= 0.19


async def test_429_twice_raises():
    def handler(request):
        return httpx.Response(429, json={"error": "slow down"})

    async with make_client(handler, pause=0.05) as client:
        with pytest.raises(LichessRateLimited):
            await client.get_json("/api/x")
        assert client.rate_limit_hits == 2


async def test_pause_applies_to_other_requests():
    """After a 429, the *next* request also waits for the pause to end."""
    codes = iter([429, 200, 200])
    times = []

    def handler(request):
        times.append(asyncio.get_event_loop().time())
        return httpx.Response(next(codes), json={})

    async with make_client(handler, pause=0.2) as client:
        await client.get_json("/api/a")  # 429 then 200
        await client.get_json("/api/b")  # should go through immediately (pause already elapsed)
    assert times[1] - times[0] >= 0.19


async def test_requests_are_serialised():
    active = 0
    max_active = 0

    async def slow_handler(request):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.02)
        active -= 1
        return httpx.Response(200, json={"path": request.url.path})

    async with make_client(slow_handler) as client:
        results = await asyncio.gather(*(client.get_json(f"/api/{i}") for i in range(5)))
    assert max_active == 1
    assert {r["path"] for r in results} == {f"/api/{i}" for i in range(5)}


async def test_stream_ndjson_and_http_error():
    body = "".join(json.dumps({"n": i}) + "\n" for i in range(3))

    def handler(request):
        if request.url.path == "/api/team/private/users":
            return httpx.Response(403, json={"error": "private"})
        assert request.headers["Accept"] == "application/x-ndjson"
        return httpx.Response(200, text=body)

    async with make_client(handler) as client:
        assert [o["n"] async for o in client.team_members("public", full=True)] == [0, 1, 2]
        with pytest.raises(LichessHTTPError) as exc:
            async for _ in client.team_members("private"):
                pass
        assert exc.value.status_code == 403


async def test_open_stream_does_not_block_other_requests():
    release = asyncio.Event()

    async def body():
        yield b'{"n": 1}\n'
        await release.wait()
        yield b'{"n": 2}\n'

    def handler(request):
        if request.url.path.startswith("/api/stream/"):
            return httpx.Response(200, content=body())
        return httpx.Response(200, json={"ok": True})

    async with make_client(handler) as client:
        stream = client.stream_game("abc")
        assert (await anext(stream))["n"] == 1
        # The stream is open and waiting; a normal request must still go through.
        assert await asyncio.wait_for(client.get_json("/api/x"), 1) == {"ok": True}
        release.set()
        assert (await anext(stream))["n"] == 2
        await stream.aclose()


async def test_transport_errors_become_lichess_errors():
    def handler(request):
        raise httpx.ConnectError("boom", request=request)

    async with make_client(handler) as client:
        with pytest.raises(LichessTransportError) as exc:
            await client.get_json("/api/x")
        assert isinstance(exc.value, LichessError)
        assert "ConnectError" in str(exc.value)
        # the client is still usable afterwards (lock released)
        with pytest.raises(LichessTransportError):
            await asyncio.wait_for(client.get_json("/api/y"), 1)


async def test_stream_dropping_mid_body_raises_transport_error():
    async def body():
        yield b'{"n": 1}\n'
        raise httpx.ReadError("connection reset")

    def handler(request):
        return httpx.Response(200, content=body())

    async with make_client(handler) as client:
        stream = client.stream_game("abc")
        assert (await anext(stream))["n"] == 1
        with pytest.raises(LichessTransportError):
            await anext(stream)


async def test_users_by_ids_posts_text_plain():
    captured = {}

    def handler(request):
        captured["method"] = request.method
        captured["content_type"] = request.headers["Content-Type"]
        captured["body"] = request.content.decode()
        return httpx.Response(200, json=[{"id": "a"}, {"id": "b"}])

    async with make_client(handler) as client:
        assert await client.users_by_ids(["a", "b"]) == [{"id": "a"}, {"id": "b"}]
        with pytest.raises(ValueError):
            await client.users_by_ids(["x"] * 301)
    assert captured == {"method": "POST", "content_type": "text/plain", "body": "a,b"}


async def test_stream_games_by_users_posts_ids():
    captured = {}

    def handler(request):
        captured.update(method=request.method, body=request.content.decode(), params=dict(request.url.params),
                        content_type=request.headers["content-type"], accept=request.headers["accept"],
                        read_timeout=request.extensions["timeout"]["read"])
        return httpx.Response(200, text='{"id": "g1"}\n')

    async with make_client(handler) as client:
        docs = [d async for d in client.stream_games_by_users(["a", "b"])]
        assert docs == [{"id": "g1"}]
        assert captured == {"method": "POST", "body": "a,b", "params": {"withCurrentGames": "true"},
                            "content_type": "text/plain", "accept": "application/x-ndjson",
                            "read_timeout": 900.0}  # idle streams must not hit the 30s request timeout
        with pytest.raises(ValueError):
            client.stream_games_by_users(["x"] * 301)
        with pytest.raises(ValueError):
            client.stream_games_by_users([])


async def test_users_status_params():
    captured = {}

    def handler(request):
        captured.update(request.url.params)
        return httpx.Response(200, json=[])

    async with make_client(handler) as client:
        await client.users_status(["a", "b"])
        assert captured == {"ids": "a,b", "withGameMetas": "true"}
        captured.clear()
        await client.users_status(["a"], with_game_metas=False)
        assert captured == {"ids": "a", "withGameIds": "true"}
