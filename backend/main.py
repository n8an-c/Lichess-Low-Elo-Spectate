"""FastAPI app: SSE endpoint, control routes and the built frontend."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .config import Settings, load_settings
from .lichess import LichessClient
from .service import Spectator
from .viewer import Broadcaster, Viewer

log = logging.getLogger(__name__)

FRONTEND_DIST = Path(__file__).resolve().parent.parent / "frontend" / "dist"
PING_SECONDS = 15


def create_app(settings: Settings, *, skip_pool_build: bool = False, client: LichessClient | None = None) -> FastAPI:
    broadcaster = Broadcaster()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        spectator = Spectator(settings, skip_pool_build=skip_pool_build, client=client)
        pool_cache: dict = {"at": 0.0, "size": 0}

        def stats() -> dict:
            now = asyncio.get_event_loop().time()
            if now - pool_cache["at"] > 5:
                try:
                    pool_cache["size"] = len(spectator.db.active_player_ids())
                except sqlite3.ProgrammingError:  # database closed (shutdown); keep the last value
                    pass
                pool_cache["at"] = now
            return {"candidates": len(spectator.candidates), "poolSize": pool_cache["size"]}

        def report_exit(task: asyncio.Task) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                log.error("%s task died: %r", task.get_name(), exc, exc_info=exc)

        viewer = Viewer(spectator.client, spectator.candidates, settings, broadcaster, stats=stats)
        spectator.on_accept.append(viewer.notify_candidate)
        app.state.spectator = spectator
        app.state.viewer = viewer
        tasks = [
            asyncio.create_task(spectator.run(), name="spectator"),
            asyncio.create_task(viewer.run(), name="viewer"),
        ]
        for task in tasks:
            task.add_done_callback(report_exit)
        try:
            yield
        finally:
            await viewer.stop()
            for task in tasks:
                task.cancel()
            for task in tasks:
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

    app = FastAPI(title="Low-ELO Lichess Spectator", lifespan=lifespan)

    @app.get("/api/state")
    async def get_state(request: Request) -> dict:
        return request.app.state.viewer.snapshot()

    @app.get("/api/config")
    async def get_config() -> dict:
        """Settings the browser needs (the engine runs entirely client-side)."""
        return {"engineDepth": settings.viewer.engine_depth, "engine": "stockfish-19-lite-single"}

    @app.get("/api/candidates")
    async def get_candidates(request: Request) -> list[dict]:
        return [
            {"id": g.id, "url": g.url, "white": g.white.__dict__, "black": g.black.__dict__,
             "pliesAtJoin": g.plies_at_join, "ageSeconds": round(asyncio.get_event_loop().time() - g.detected_at)}
            for g in request.app.state.spectator.candidates.snapshot()
        ]

    @app.post("/api/next")
    async def post_next(request: Request) -> dict:
        return await request.app.state.viewer.next()

    @app.post("/api/watch/{game_id}")
    async def post_watch(game_id: str, request: Request, force: bool = False) -> dict:
        if not game_id.isalnum() or len(game_id) < 8:
            raise HTTPException(400, "game_id must be an 8+ character Lichess game id")
        return await request.app.state.viewer.watch(game_id[:8], force=force)

    @app.get("/api/events")
    async def sse(request: Request) -> StreamingResponse:
        viewer: Viewer = request.app.state.viewer

        async def gen():
            yield _sse("state", viewer.snapshot())
            agen = broadcaster.subscribe()
            try:
                while True:
                    try:
                        event, data = await asyncio.wait_for(anext(agen), PING_SECONDS)
                    except asyncio.TimeoutError:
                        yield ": ping\n\n"
                        continue
                    except StopAsyncIteration:
                        return
                    yield _sse(event, data)
            finally:
                await agen.aclose()

        return StreamingResponse(
            gen(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    if FRONTEND_DIST.is_dir():
        app.mount("/", StaticFiles(directory=FRONTEND_DIST, html=True), name="frontend")
    else:

        @app.get("/", response_class=PlainTextResponse)
        async def no_frontend() -> str:
            return (
                "Frontend not built. Run `cd frontend && npm install && npm run build`, then restart.\n"
                "API: GET /api/state, GET /api/events (SSE), POST /api/next, POST /api/watch/{game_id}?force=true\n"
            )

    return app


def _sse(event: str, data) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


def _default_app() -> FastAPI:
    """App for `uvicorn backend.main:app`; reads SPECTATE_CONFIG or ./config.toml if present."""
    path = Path(os.environ.get("SPECTATE_CONFIG", "config.toml"))
    settings = load_settings(path) if path.exists() else Settings()
    return create_app(settings)


app = _default_app()
