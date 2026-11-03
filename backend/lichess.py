"""Rate-limited async Lichess API client.

Rules (from https://lichess.org/api, "Rate limiting"):
- Only make one request at a time. A single asyncio.Lock serialises every
  request and every open NDJSON stream, which acts as the global queue.
- On HTTP 429, pause *all* requests for a full minute, then retry once.
- Wait a small courtesy delay after each request.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx

from .config import LichessSettings

log = logging.getLogger(__name__)

BASE_URL = "https://lichess.org"
NDJSON = "application/x-ndjson"


class LichessError(Exception):
    """Base class for client errors."""


class LichessRateLimited(LichessError):
    """Raised when a request is still rejected with 429 after the pause."""


class LichessTransportError(LichessError):
    """A network-level failure (timeout, connection reset, protocol error)."""


class LichessHTTPError(LichessError):
    def __init__(self, response: httpx.Response):
        self.status_code = response.status_code
        self.url = str(response.request.url)
        super().__init__(f"HTTP {self.status_code} for {self.url}")


class LichessClient:
    def __init__(self, settings: LichessSettings | None = None, transport: httpx.AsyncBaseTransport | None = None):
        self.settings = settings or LichessSettings()
        headers = {"User-Agent": self.settings.user_agent}
        if self.settings.token:
            headers["Authorization"] = f"Bearer {self.settings.token}"
        self._http = httpx.AsyncClient(base_url=BASE_URL, headers=headers, timeout=30.0, transport=transport)
        self._lock = asyncio.Lock()
        self._pause_until = 0.0
        self.rate_limit_hits = 0
        self.request_count = 0

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "LichessClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    # -- queue / pause mechanics -------------------------------------------------

    async def _wait_if_paused(self) -> None:
        remaining = self._pause_until - time.monotonic()
        if remaining > 0:
            log.warning("Rate-limit pause: waiting %.0fs before the next Lichess request", remaining)
            await asyncio.sleep(remaining)

    def _enter_pause(self) -> None:
        self.rate_limit_hits += 1
        self._pause_until = time.monotonic() + self.settings.rate_limit_pause_seconds
        log.warning(
            "Lichess returned 429; pausing all requests for %.0fs", self.settings.rate_limit_pause_seconds
        )

    async def _courtesy_delay(self) -> None:
        if self.settings.courtesy_delay_seconds > 0:
            await asyncio.sleep(self.settings.courtesy_delay_seconds)

    async def _send(self, request: httpx.Request, *, stream: bool = False) -> httpx.Response:
        """Send one request under the global lock, handling 429 with one retry.

        The caller must already hold ``self._lock``.
        """
        for attempt in (1, 2):
            await self._wait_if_paused()
            self.request_count += 1
            log.debug("Lichess %s %s (attempt %d)", request.method, request.url, attempt)
            try:
                response = await self._http.send(request, stream=stream)
            except httpx.HTTPError as exc:  # timeouts, connection errors, protocol errors
                raise LichessTransportError(f"{type(exc).__name__}: {exc or request.url}") from exc
            if response.status_code != 429:
                return response
            await response.aclose()
            self._enter_pause()
        raise LichessRateLimited(f"still rate limited after pause: {request.method} {request.url}")

    # -- generic request helpers -------------------------------------------------

    async def request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        content: str | None = None,
        content_type: str | None = None,
    ) -> Any:
        headers = {"Accept": "application/json"}
        if content_type:
            headers["Content-Type"] = content_type
        request = self._http.build_request(method, path, params=params, content=content, headers=headers)
        async with self._lock:
            try:
                response = await self._send(request)
                if response.status_code >= 400:
                    raise LichessHTTPError(response)
                return response.json()
            finally:
                await self._courtesy_delay()

    async def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return await self.request_json("GET", path, params=params)

    async def post_text_json(self, path: str, body: str, params: dict[str, Any] | None = None) -> Any:
        return await self.request_json("POST", path, params=params, content=body, content_type="text/plain")

    async def stream_ndjson(self, path: str, params: dict[str, Any] | None = None) -> AsyncIterator[dict]:
        """Yield one parsed JSON object per NDJSON line.

        The global lock is held only until the response headers arrive (plus
        the courtesy delay); the body is then read with the lock released so
        that a long-lived stream (a game, a large team) does not block the
        poller. Lichess's "one request at a time" rule applies to opening
        requests; open game streams have their own separate limit (8 per IP).
        """
        request = self._http.build_request("GET", path, params=params, headers={"Accept": NDJSON})
        async with self._lock:
            response = await self._send(request, stream=True)
            try:
                if response.status_code >= 400:
                    await response.aread()
                    await response.aclose()
                    raise LichessHTTPError(response)
            finally:
                await self._courtesy_delay()
        try:
            async for line in response.aiter_lines():
                line = line.strip()
                if line:
                    yield json.loads(line)
        except httpx.HTTPError as exc:
            raise LichessTransportError(f"{type(exc).__name__} while streaming {request.url}: {exc}") from exc
        finally:
            await response.aclose()

    # -- typed endpoint wrappers -------------------------------------------------

    async def search_teams(self, text: str, page: int = 1) -> dict:
        """GET /api/team/search -> paginator with ``currentPageResults`` and ``nextPage``."""
        return await self.get_json("/api/team/search", {"text": text, "page": page})

    def team_members(self, team_id: str, full: bool = False) -> AsyncIterator[dict]:
        """GET /api/team/{teamId}/users as an NDJSON stream.

        ``full=True`` returns full user documents (with ``perfs`` and
        ``seenAt``) but Lichess then caps the stream at the 1,000 most recent
        members instead of 5,000.
        """
        params = {"full": "true"} if full else None
        return self.stream_ndjson(f"/api/team/{team_id}/users", params)

    async def users_by_ids(self, ids: list[str]) -> list[dict]:
        """POST /api/users with a comma-separated body; at most 300 IDs.

        Lichess limits this endpoint to 8,000 users per 10 minutes and
        120,000 per day.
        """
        if len(ids) > 300:
            raise ValueError("users_by_ids accepts at most 300 IDs per call")
        if not ids:
            return []
        return await self.post_text_json("/api/users", ",".join(ids))

    async def users_status(self, ids: list[str], with_game_metas: bool = True) -> list[dict]:
        """GET /api/users/status for up to 100 IDs (used by the milestone 2 poller).

        With ``with_game_metas`` each playing user carries
        ``playing: {"id": ..., "clock": "3+0", "variant": ...}``; otherwise
        ``withGameIds`` is used and the game ID is in ``playingId``.
        """
        if len(ids) > 100:
            raise ValueError("users_status accepts at most 100 IDs per call")
        if not ids:
            return []
        params: dict[str, Any] = {"ids": ",".join(ids)}
        params["withGameMetas" if with_game_metas else "withGameIds"] = "true"
        return await self.get_json("/api/users/status", params)

    def stream_game(self, game_id: str) -> AsyncIterator[dict]:
        """GET /api/stream/game/{id} (used from milestone 2).

        First line: game description (no clock field; see SPEC notes).
        Following lines: ``{"fen", "lm", "wc", "bc"}``. Ongoing games are
        delayed by 3 moves and at most 8 streams per IP may be open.
        """
        return self.stream_ndjson(f"/api/stream/game/{game_id}")
