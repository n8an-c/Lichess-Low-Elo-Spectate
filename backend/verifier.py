"""Game verification and the in-memory candidate list.

A game ID found by the poller is verified by opening its stream and reading
the first messages. Facts about ``GET /api/stream/game/{id}`` (checked live):

- Line 1 describes the game: id, variant, speed, perf, rated, createdAt and
  players with ratings. It carries no clock, ply count or status while the
  game is running. A finished game has ``ratingDiff`` on both players.
- The stream then replays the game from the initial position, one
  ``{fen, lm, wc, bc}`` line per move, in a quick burst, before going live.
  The current ply count is therefore read from the last FEN of that burst.
- Ongoing games are delayed by 3 moves on Lichess's side.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from .config import Settings
from .lichess import LichessClient, LichessError, LichessHTTPError

log = logging.getLogger(__name__)


def plies_from_fen(fen: str) -> int:
    """Number of half-moves played, derived from the FEN's fullmove number and side to move."""
    fields = fen.split()
    if len(fields) < 6:
        raise ValueError(f"not a full FEN: {fen!r}")
    side, fullmove = fields[1], int(fields[5])
    return (fullmove - 1) * 2 + (1 if side == "b" else 0)


@dataclass(frozen=True)
class PlayerInfo:
    id: str
    name: str
    rating: int
    title: str | None = None


@dataclass
class GameInfo:
    id: str
    white: PlayerInfo
    black: PlayerInfo
    created_at: int  # epoch ms
    plies_at_join: int
    detected_at: float = field(default_factory=time.monotonic)
    found_at: float = field(default_factory=time.time)  # epoch seconds, for display
    fen: str | None = None

    @property
    def url(self) -> str:
        return f"https://lichess.org/{self.id}"

    def to_dict(self, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        return {
            "id": self.id,
            "url": self.url,
            "white": self.white.__dict__,
            "black": self.black.__dict__,
            "pliesAtJoin": self.plies_at_join,
            "createdAt": self.created_at,
            "foundAt": self.found_at,
            "ageSeconds": round(now - self.created_at / 1000) if self.created_at else None,
        }

    def describe(self) -> str:
        return (
            f"{self.id}: {self.white.name} ({self.white.rating}) vs "
            f"{self.black.name} ({self.black.rating}), {self.plies_at_join} plies"
        )


@dataclass(frozen=True)
class Verdict:
    accepted: bool
    reason: str
    game: GameInfo | None = None


def _player(side: dict) -> PlayerInfo | None:
    user = side.get("user") or {}
    if "rating" not in side or "id" not in user:
        return None  # anonymous or AI player
    return PlayerInfo(id=user["id"], name=user.get("name", user["id"]), rating=int(side["rating"]), title=user.get("title"))


def header_players(head: dict) -> tuple[PlayerInfo | None, PlayerInfo | None]:
    """Both players of a game stream header (None for anonymous / AI sides)."""
    players = head.get("players") or {}
    return _player(players.get("white") or {}), _player(players.get("black") or {})


def is_rated_standard_blitz(head: dict) -> bool:
    """True when the header's ratings are blitz ratings worth learning from."""
    return bool(head.get("rated")) and (head.get("variant") or {}).get("key") == "standard" and head.get("speed") == "blitz"


DiscoverCallback = Callable[[str, int], None]  # (player id, rating in this game)


def check_header(head: dict, settings: Settings, now_ms: int, *, check_age: bool = True) -> Verdict:
    """Apply every check that can be made from the stream's first line.

    ``check_age=False`` skips the "started recently" rule; the viewer uses
    that when re-checking a candidate at pick time, where the ply count is
    the late-join criterion.
    """
    if not head.get("rated"):
        return Verdict(False, "not rated")
    variant = (head.get("variant") or {}).get("key")
    if variant != "standard":
        return Verdict(False, f"variant {variant}")
    if head.get("speed") != "blitz":
        return Verdict(False, f"speed {head.get('speed')}")
    players = head.get("players") or {}
    white, black = header_players(head)
    if white is None or black is None:
        return Verdict(False, "anonymous or computer player")
    cap = settings.pool.rating_cap
    for p in (white, black):
        if p.rating >= cap:
            return Verdict(False, f"{p.name} rated {p.rating} >= {cap}")
    if "ratingDiff" in players["white"] or "ratingDiff" in players["black"]:
        return Verdict(False, "already finished")
    created_at = int(head.get("createdAt") or 0)
    age = (now_ms - created_at) / 1000 if created_at else None
    if check_age and age is not None and age > settings.detection.max_game_age_seconds:
        return Verdict(False, f"started {age:.0f}s ago")
    game = GameInfo(id=head["id"], white=white, black=black, created_at=created_at, plies_at_join=0)
    return Verdict(True, "header ok", game)


class StreamEnded(Exception):
    """The game stream closed (normally after the final game description).

    ``description`` holds that final game description line when one was seen,
    and ``last_position`` the last ``{fen, lm?, wc, bc}`` line before it.
    """

    def __init__(self, description: dict | None = None, last_position: dict | None = None):
        super().__init__("stream ended")
        self.description = description
        self.last_position = last_position


class StreamReader:
    """Pumps an NDJSON stream into a queue so callers can wait with a timeout.

    ``asyncio.wait_for`` on ``anext(stream)`` would cancel (and so destroy)
    the underlying generator on timeout; waiting on a queue instead keeps the
    stream alive, which the viewer needs to detect the end of the replay
    burst and then keep following the game.
    """

    def __init__(self, stream):
        self._stream = stream
        self._queue: asyncio.Queue[tuple[str, object]] = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self.closed = False

    def start(self) -> "StreamReader":
        self._task = asyncio.create_task(self._pump())
        return self

    async def _pump(self) -> None:
        try:
            async for line in self._stream:
                await self._queue.put(("line", line))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # surfaced to the consumer as an error item
            await self._queue.put(("error", exc))
        finally:
            await self._queue.put(("end", None))

    async def next(self, timeout: float | None) -> dict | None:
        """Next parsed line, or None on timeout. Raises StreamEnded or the stream's error."""
        if self.closed:
            raise StreamEnded()
        try:
            kind, payload = await asyncio.wait_for(self._queue.get(), timeout)
        except asyncio.TimeoutError:
            return None
        if kind == "line":
            return payload  # type: ignore[return-value]
        self.closed = True
        if kind == "error":
            raise payload  # type: ignore[misc]
        raise StreamEnded()

    async def close(self) -> None:
        self.closed = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        await self._stream.aclose()


def is_game_description(line: dict) -> bool:
    return "id" in line and "players" in line


async def read_replay(reader: StreamReader, det, *, max_plies: int | None = None) -> dict | None:
    """Consume the replay burst and return the last position line seen.

    The line looks like ``{"fen", "lm"?, "wc", "bc"}``. Returns None if the
    burst ended with no position. Raises StreamEnded (carrying the final
    game description, if any) when the game is already over, and
    ``ValueError`` with a reason for the clock / ply early-exit checks.
    """
    last: dict | None = None
    first_clock_checked = False
    while True:
        line = await reader.next(det.replay_idle_seconds)
        if line is None:
            return last  # replay burst finished; the game is live from here
        if is_game_description(line):
            raise StreamEnded(line, last)
        if "fen" not in line:
            continue
        last = line
        last_fen = line["fen"]
        if not first_clock_checked and "wc" in line and "bc" in line:
            first_clock_checked = True
            expected = _initial_seconds(det.time_control)
            if expected is not None and "lm" not in line and (line["wc"] != expected or line["bc"] != expected):
                raise ValueError(f"initial clock {line['wc']}+{line['bc']}s is not {expected}s")
        if max_plies is not None:
            plies = plies_from_fen(last_fen)
            if plies > max_plies:
                raise ValueError(f"{plies} plies already played")


async def open_game_stream(client: LichessClient, game_id: str, det) -> tuple[StreamReader, dict]:
    """Open a game stream and return (reader, header line).

    Raises LichessHTTPError / LichessError on request failure, StreamEnded on
    an empty stream and asyncio.TimeoutError if nothing arrives in time.
    The reader is closed before raising.
    """
    reader = StreamReader(client.stream_game(game_id)).start()
    try:
        head = await reader.next(det.stream_open_timeout_seconds)
        if head is None:
            raise asyncio.TimeoutError()
        return reader, head
    except BaseException:
        await reader.close()
        raise


async def verify_game(
    client: LichessClient,
    game_id: str,
    settings: Settings,
    now_ms: int | None = None,
    discover: DiscoverCallback | None = None,
) -> Verdict:
    """Open the game stream, check the header, then count the replayed plies.

    ``discover`` is called with (player id, rating) for both players of every
    rated standard blitz header, accepted or not, so opponents can be
    recorded as pool candidates.
    """
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    det = settings.detection
    try:
        reader, head = await open_game_stream(client, game_id, det)
    except StreamEnded:
        return Verdict(False, "empty stream")
    except asyncio.TimeoutError:
        return Verdict(False, "stream open timed out")
    except LichessHTTPError as exc:
        return Verdict(False, f"HTTP {exc.status_code}")
    except LichessError as exc:
        return Verdict(False, str(exc))
    try:
        if discover is not None and is_rated_standard_blitz(head):
            for p in header_players(head):
                if p is not None:
                    try:
                        discover(p.id, p.rating)
                    except Exception:
                        log.exception("discover callback failed for %s", p.id)
        # The age rule is applied after the replay: a game still at ply 0 is
        # never "too old" (games arrive from the stream the moment they are
        # created, sometimes before anyone has moved).
        verdict = check_header(head, settings, now, check_age=False)
        if not verdict.accepted:
            return verdict
        game = verdict.game
        assert game is not None
        try:
            last = await read_replay(reader, det, max_plies=det.max_start_plies)
        except StreamEnded:
            return Verdict(False, "stream ended (game over)")
        except ValueError as exc:
            return Verdict(False, str(exc))
        except LichessError as exc:
            return Verdict(False, str(exc))
        if last is None:
            return Verdict(False, "no position received")
        plies = plies_from_fen(last["fen"])
        age = (now - game.created_at) / 1000 if game.created_at else None
        if plies > 0 and age is not None and age > det.max_game_age_seconds:
            return Verdict(False, f"started {age:.0f}s ago")
        game.plies_at_join = plies
        game.fen = last["fen"]
        return Verdict(True, "ok", game)
    finally:
        await reader.close()


def _initial_seconds(time_control: str) -> int | None:
    """'3+0' -> 180. Returns None if the string is not minutes+increment."""
    try:
        minutes, _ = time_control.split("+")
        return int(float(minutes) * 60)
    except ValueError:
        return None


class Candidates:
    """Accepted games, newest first, expiring after a TTL."""

    def __init__(self, ttl_seconds: float):
        self.ttl = ttl_seconds
        self._games: list[GameInfo] = []

    def add(self, game: GameInfo) -> None:
        self._games = [g for g in self._games if g.id != game.id]
        self._games.insert(0, game)

    def expire(self, now: float | None = None) -> list[GameInfo]:
        now = time.monotonic() if now is None else now
        expired = [g for g in self._games if now - g.detected_at > self.ttl]
        if expired:
            self._games = [g for g in self._games if now - g.detected_at <= self.ttl]
        return expired

    def newest(self) -> GameInfo | None:
        return self._games[0] if self._games else None

    def pop_newest(self) -> GameInfo | None:
        return self._games.pop(0) if self._games else None

    def remove(self, game_id: str) -> None:
        self._games = [g for g in self._games if g.id != game_id]

    def snapshot(self) -> list[GameInfo]:
        return list(self._games)

    def __len__(self) -> int:
        return len(self._games)
