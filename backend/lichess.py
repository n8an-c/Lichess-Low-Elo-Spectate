"""Rate-limited async Lichess API client.

Rules (from https://lichess.org/api, "Rate limiting"):
- Only make one request at a time. A single asyncio.Lock serialises the
  opening of every request and every NDJSON stream (the global queue).
- Consecutive requests are spaced by ``courtesy_delay_seconds``; the spacing
  is enforced before a request starts, so it costs nothing when the caller
  had other work to do in between.
- On HTTP 429, pause *all* requests (for ``Retry-After`` or a full minute,
  whichever is longer), then retry once.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from collections.abc import AsyncIterator
from typing import Any

import httpx

from .config import LichessSettings

log = logging.getLogger(__name__)

BASE_URL = "https://lichess.org"
NDJSON = "application/x-ndjson"
STREAM_READ_TIMEOUT = 900.0  # seconds without a byte before an NDJSON stream is considered dead


def retry_after_seconds(value: str | None) -> float:
    """Seconds from a Retry-After header (delay-seconds or an HTTP date); 0 if absent or unreadable."""
    if not value:
        return 0.0
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        from datetime import datetime, timezone
        from email.utils import parsedate_to_datetime

        when = parsedate_to_datetime(value)
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, IndexError):
        return 0.0


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
        self._next_allowed_at = 0.0
        self._recent: deque[float] = deque(maxlen=600)  # monotonic send times, for the stats line
        self.rate_limit_hits = 0
        self.request_count = 0

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "LichessClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    # -- queue / pause / spacing mechanics -----------------------------------------

    def requests_per_minute(self) -> float:
        cutoff = time.monotonic() - 60
        return float(sum(1 for t in self._recent if t >= cutoff))

    async def _wait_for_slot(self) -> None:
        remaining = self._pause_until - time.monotonic()
        if remaining > 0:
            log.warning("Rate-limit pause: waiting %.0fs before the next Lichess request", remaining)
            await asyncio.sleep(remaining)
        spacing = self._next_allowed_at - time.monotonic()
        if spacing > 0:
            await asyncio.sleep(spacing)

    def _mark_sent(self) -> None:
        now = time.monotonic()
        self._next_allowed_at = now + self.settings.courtesy_delay_seconds
        self._recent.append(now)

    def _enter_pause(self, retry_after: str | None = None) -> None:
        self.rate_limit_hits += 1
        seconds = max(self.settings.rate_limit_pause_seconds, retry_after_seconds(retry_after))
        self._pause_until = time.monotonic() + seconds
        log.warning("Lichess returned 429; pausing all requests for %.0fs", seconds)

    async def _send(self, request: httpx.Request, *, stream: bool = False) -> httpx.Response:
        """Send one request under the global lock, handling 429 with one retry.

        The caller must already hold ``self._lock``.
        """
        for attempt in (1, 2):
            await self._wait_for_slot()
            self.request_count += 1
            log.debug("Lichess %s %s (attempt %d)", request.method, request.url, attempt)
            try:
                response = await self._http.send(request, stream=stream)
            except httpx.HTTPError as exc:  # timeouts, connection errors, protocol errors
                self._mark_sent()
                raise LichessTransportError(f"{type(exc).__name__}: {exc or request.url}") from exc
            self._mark_sent()
            if response.status_code != 429:
                return response
            retry_after = response.headers.get("Retry-After")
            await response.aclose()
            self._enter_pause(retry_after)
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
            response = await self._send(request)
        if response.status_code >= 400:
            raise LichessHTTPError(response)
        return response.json()

    async def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return await self.request_json("GET", path, params=params)

    async def post_text_json(self, path: str, body: str, params: dict[str, Any] | None = None) -> Any:
        return await self.request_json("POST", path, params=params, content=body, content_type="text/plain")

    async def stream_ndjson(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        method: str = "GET",
        content: str | None = None,
        content_type: str | None = None,
    ) -> AsyncIterator[dict]:
        """Yield one parsed JSON object per NDJSON line.

        The global lock is held only until the response headers arrive; the
        body is then read with the lock released so that a long-lived stream
        (a game, a games export, a team roster) does not block the poller.
        Lichess's "one request at a time" rule applies to opening requests;
        open game streams have their own separate limit (8 per IP).
        """
        headers = {"Accept": NDJSON}
        if content_type:
            headers["Content-Type"] = content_type
        # Streams can legitimately stay silent for a long time (games-by-users
        # only speaks when a game starts or ends), so only the connect phase
        # keeps the normal timeout; reads get a generous one and the callers
        # reconnect if a stream really goes quiet for that long.
        request = self._http.build_request(
            method, path, params=params, content=content, headers=headers,
            timeout=httpx.Timeout(30.0, read=STREAM_READ_TIMEOUT),
        )
        async with self._lock:
            response = await self._send(request, stream=True)
            if response.status_code >= 400:
                await response.aread()
                await response.aclose()
                raise LichessHTTPError(response)
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
        """GET /api/users/status for up to 100 IDs.

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

    def user_games(
        self,
        username: str,
        *,
        rated: bool | None = True,
        perf_type: str | None = "blitz",
        max_games: int = 75,
        since_ms: int | None = None,
        until_ms: int | None = None,
        moves: bool = False,
    ) -> AsyncIterator[dict]:
        """GET /api/games/user/{username} as NDJSON, newest first.

        Each game carries ``players.{white,black}.{user.id,user.name,user.title?,rating,provisional?}``,
        ``clock.{initial,increment}``, ``rated``, ``variant``, ``speed``,
        ``createdAt``. Lichess throttles the stream to 20 games/s anonymously
        (30/s with a token) and rounds ``max`` above 25 up to a multiple of 25.
        """
        params: dict[str, Any] = {"max": max_games, "moves": "true" if moves else "false"}
        if rated is not None:
            params["rated"] = "true" if rated else "false"
        if perf_type:
            params["perfType"] = perf_type
        if since_ms is not None:
            params["since"] = since_ms
        if until_ms is not None:
            params["until"] = until_ms
        return self.stream_ndjson(f"/api/games/user/{username}", params)

    async def current_tournaments(self) -> dict:
        """GET /api/tournament -> {"created": [...], "started": [...], "finished": [...]}."""
        return await self.get_json("/api/tournament")

    def arena_results(self, arena_id: str, nb: int = 300) -> AsyncIterator[dict]:
        """GET /api/tournament/{id}/results as NDJSON: {rank, score, rating, username, ...}."""
        return self.stream_ndjson(f"/api/tournament/{arena_id}/results", {"nb": nb})

    def stream_games_by_users(self, ids: list[str], with_current_games: bool = True) -> AsyncIterator[dict]:
        """POST /api/stream/games-by-users: games between the listed users, as they start and finish.

        At most 300 IDs. Lichess only emits games where *both* players are in
        the list. With ``with_current_games`` the games already in progress are
        sent first. Each line is a game document with ``clock``, ``rated``,
        ``variant``, ``statusName`` and ``players.{white,black}.{userId,rating}``.
        """
        if len(ids) > 300:
            raise ValueError("stream_games_by_users accepts at most 300 IDs per stream")
        if not ids:
            raise ValueError("stream_games_by_users needs at least one ID")
        params = {"withCurrentGames": "true"} if with_current_games else None
        return self.stream_ndjson(
            "/api/stream/games-by-users", params, method="POST", content=",".join(ids), content_type="text/plain"
        )

    def stream_game(self, game_id: str) -> AsyncIterator[dict]:
        """GET /api/stream/game/{id}.

        First line: game description (no clock field; see SPEC notes).
        Following lines: ``{"fen", "lm", "wc", "bc"}``. Ongoing games are
        delayed by 3 moves and at most 8 streams per IP may be open.
        """
        return self.stream_ndjson(f"/api/stream/game/{game_id}")
