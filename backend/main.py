"""FastAPI app: a JSON feed of the games found, and a page that links to them on lichess.org."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, PlainTextResponse

from .config import Settings, load_settings
from .lichess import LichessClient
from .service import Spectator

log = logging.getLogger(__name__)

FRONTEND_INDEX = Path(__file__).resolve().parent.parent / "frontend" / "index.html"


def create_app(settings: Settings, *, skip_pool_build: bool = False, client: LichessClient | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        spectator = Spectator(settings, skip_pool_build=skip_pool_build, client=client)
        app.state.spectator = spectator
        task = asyncio.create_task(spectator.run(), name="spectator")

        def report_exit(t: asyncio.Task) -> None:
            if not t.cancelled() and t.exception() is not None:
                log.error("spectator task died: %r", t.exception(), exc_info=t.exception())

        task.add_done_callback(report_exit)
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    app = FastAPI(title="Low-ELO Lichess Spectator", lifespan=lifespan)

    @app.get("/api/games")
    async def get_games(request: Request, limit: int = Query(50, ge=1, le=200)) -> dict:
        """Recent qualifying games, newest first, with links to lichess.org."""
        return request.app.state.spectator.feed(limit)

    @app.get("/api/candidates")
    async def get_candidates(request: Request) -> list[dict]:
        """Accepted games still within the late-join window (debug)."""
        return [g.to_dict() for g in request.app.state.spectator.candidates.snapshot()]

    @app.get("/")
    async def index():
        if FRONTEND_INDEX.is_file():
            return FileResponse(FRONTEND_INDEX, media_type="text/html", headers={"Cache-Control": "no-cache"})
        return PlainTextResponse(f"frontend/index.html not found (expected at {FRONTEND_INDEX}). API: GET /api/games\n")

    return app


def _default_app() -> FastAPI:
    """App for `uvicorn backend.main:app`; reads SPECTATE_CONFIG or ./config.toml if present."""
    path = Path(os.environ.get("SPECTATE_CONFIG", "config.toml"))
    settings = load_settings(path) if path.exists() else Settings()
    return create_app(settings)


app = _default_app()
