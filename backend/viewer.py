"""The viewer: follows one game at a time and broadcasts it to SSE subscribers."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any

from .config import Settings
from .lichess import LichessClient, LichessError, LichessHTTPError
from .verifier import (
    Candidates,
    GameInfo,
    StreamEnded,
    StreamReader,
    check_header,
    is_game_description,
    open_game_stream,
    plies_from_fen,
    read_replay,
)

log = logging.getLogger(__name__)

LIVE_STATUSES = {"created", "started"}
SNAPSHOT_EVENTS = {"state", "game", "move", "end", "waiting"}


class Broadcaster:
    """Fan-out of (event, data) pairs to any number of subscribers."""

    def __init__(self, max_queue: int = 256):
        self.max_queue = max_queue
        self._subscribers: set[asyncio.Queue] = set()

    def publish(self, event: str, data: Any) -> None:
        for queue in list(self._subscribers):
            try:
                queue.put_nowait((event, data))
            except asyncio.QueueFull:
                log.warning("dropping slow SSE subscriber")
                self._subscribers.discard(queue)
                queue.get_nowait()  # make room for the close sentinel
                queue.put_nowait(None)

    async def subscribe(self) -> AsyncIterator[tuple[str, Any]]:
        queue: asyncio.Queue = asyncio.Queue(self.max_queue)
        self._subscribers.add(queue)
        try:
            while True:
                item = await queue.get()
                if item is None:
                    return
                yield item
        finally:
            self._subscribers.discard(queue)

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)


@dataclass
class ViewerState:
    status: str = "waiting"  # waiting | live | finished
    game: GameInfo | None = None
    fen: str | None = None
    last_move: str | None = None
    plies: int = 0
    wc: int | None = None
    bc: int | None = None
    result: dict | None = None
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        game = None
        if self.game is not None:
            game = {
                "id": self.game.id,
                "url": self.game.url,
                "white": self.game.white.__dict__,
                "black": self.game.black.__dict__,
                "createdAt": self.game.created_at,
                "pliesAtJoin": self.game.plies_at_join,
            }
        return {
            "status": self.status,
            "game": game,
            "fen": self.fen,
            "lastMove": self.last_move,
            "plies": self.plies,
            "wc": self.wc,
            "bc": self.bc,
            "result": self.result,
            "updatedAt": self.updated_at,
        }


class Viewer:
    def __init__(self, client: LichessClient, candidates: Candidates, settings: Settings, broadcaster: Broadcaster):
        self.client = client
        self.candidates = candidates
        self.settings = settings
        self.broadcaster = broadcaster
        self.state = ViewerState()
        self._wake = asyncio.Event()
        self._reader: StreamReader | None = None
        self._follow_task: asyncio.Task | None = None
        self._forced: tuple[str, bool] | None = None
        self._lock = asyncio.Lock()

    # -- external triggers ---------------------------------------------------------

    def notify_candidate(self, game: GameInfo) -> None:
        """Registered on Spectator.on_accept; wakes a waiting viewer."""
        self._wake.set()

    async def next(self) -> dict:
        """Drop the current game (if any) and pick the next candidate."""
        await self._stop_following()
        self._set_waiting()
        self._wake.set()
        return self.state.to_dict()

    async def watch(self, game_id: str, force: bool = False) -> dict:
        """Follow a specific game. ``force`` skips the rating/plies checks (debug aid)."""
        await self._stop_following()
        self._set_waiting()
        self._forced = (game_id, force)
        self._wake.set()
        return self.state.to_dict()

    # -- main loop -----------------------------------------------------------------

    async def run(self) -> None:
        while True:
            if self.state.status == "waiting":
                picked = await self._pick()
                if not picked:
                    self._wake.clear()
                    try:
                        await asyncio.wait_for(self._wake.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        pass
            else:
                self._wake.clear()
                await self._wake.wait()

    async def _pick(self) -> bool:
        """Try candidates (or a forced game) until one can be followed."""
        if self._forced is not None:
            game_id, force = self._forced
            self._forced = None
            return await self._start_game(game_id, force=force)
        self.candidates.expire()
        while (candidate := self.candidates.pop_newest()) is not None:
            if await self._start_game(candidate.id):
                return True
        return False

    async def _start_game(self, game_id: str, force: bool = False) -> bool:
        det = self.settings.detection
        try:
            reader, head = await open_game_stream(self.client, game_id, det)
        except (StreamEnded, asyncio.TimeoutError, LichessError) as exc:
            log.info("viewer: cannot open %s (%s)", game_id, exc or type(exc).__name__)
            return False
        try:
            verdict = check_header(head, self.settings, int(time.time() * 1000), check_age=False)
            if not verdict.accepted and not force:
                log.info("viewer: skipped %s: %s", game_id, verdict.reason)
                await reader.close()
                return False
            game = verdict.game or _game_from_header(head)
            try:
                fen = await read_replay(reader, det, max_plies=None if force else det.max_start_plies)
            except StreamEnded:
                log.info("viewer: skipped %s: already over", game_id)
                await reader.close()
                return False
            except ValueError as exc:
                log.info("viewer: skipped %s: %s", game_id, exc)
                await reader.close()
                return False
        except Exception:
            await reader.close()
            raise
        game.plies_at_join = plies_from_fen(fen) if fen else 0
        game.fen = fen
        self.state = ViewerState(status="live", game=game, fen=fen, plies=game.plies_at_join)
        self._reader = reader
        self._follow_task = asyncio.create_task(self._follow(reader, game.id), name=f"follow-{game.id}")
        log.info("viewer: now showing %s", game.describe())
        self.broadcaster.publish("game", self.state.to_dict())
        return True

    async def _follow(self, reader: StreamReader, game_id: str) -> None:
        try:
            while True:
                line = await reader.next(None)
                if line is None:
                    continue
                if is_game_description(line):
                    status = (line.get("status") or {}).get("name")
                    if status in LIVE_STATUSES:
                        continue
                    self._finish({"status": status or "unknownFinish", "winner": line.get("winner")}, line.get("fen"))
                    return
                if "fen" in line:
                    self.state.fen = line["fen"]
                    self.state.last_move = line.get("lm")
                    self.state.plies = plies_from_fen(line["fen"])
                    self.state.wc = line.get("wc")
                    self.state.bc = line.get("bc")
                    self.state.updated_at = time.time()
                    self.broadcaster.publish(
                        "move",
                        {"fen": self.state.fen, "lastMove": self.state.last_move, "plies": self.state.plies,
                         "wc": self.state.wc, "bc": self.state.bc},
                    )
        except StreamEnded:
            self._finish({"status": "unknownFinish", "winner": None}, None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("viewer: stream for %s failed: %s", game_id, exc)
            self._finish({"status": "disconnected", "winner": None}, None)
        finally:
            await reader.close()

    def _finish(self, result: dict, fen: str | None) -> None:
        if fen:
            self.state.fen = fen
            self.state.plies = plies_from_fen(fen)
        self.state.status = "finished"
        self.state.result = result
        self.state.updated_at = time.time()
        log.info("viewer: game %s over (%s, winner %s)", self.state.game.id if self.state.game else "?",
                 result["status"], result.get("winner"))
        self.broadcaster.publish("end", self.state.to_dict())

    async def _stop_following(self) -> None:
        task, self._follow_task = self._follow_task, None
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if self._reader is not None:
            await self._reader.close()
            self._reader = None

    def _set_waiting(self) -> None:
        self.state = ViewerState(status="waiting")
        self.broadcaster.publish("waiting", self.state.to_dict())

    async def stop(self) -> None:
        await self._stop_following()


def _game_from_header(head: dict) -> GameInfo:
    """Build a GameInfo for a forced game even if the header failed the checks."""
    from .verifier import PlayerInfo

    def player(side: dict) -> PlayerInfo:
        user = side.get("user") or {}
        name = user.get("name") or ("Stockfish" if "aiLevel" in side else "Anonymous")
        return PlayerInfo(id=user.get("id", name.lower()), name=name, rating=int(side.get("rating") or 0),
                          title=user.get("title"))

    players = head.get("players") or {}
    return GameInfo(
        id=head["id"],
        white=player(players.get("white") or {}),
        black=player(players.get("black") or {}),
        created_at=int(head.get("createdAt") or 0),
        plies_at_join=0,
    )
