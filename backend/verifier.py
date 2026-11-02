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
from contextlib import aclosing
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
    fen: str | None = None

    @property
    def url(self) -> str:
        return f"https://lichess.org/{self.id}"

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


def check_header(head: dict, settings: Settings, now_ms: int) -> Verdict:
    """Apply every check that can be made from the stream's first line."""
    if not head.get("rated"):
        return Verdict(False, "not rated")
    variant = (head.get("variant") or {}).get("key")
    if variant != "standard":
        return Verdict(False, f"variant {variant}")
    if head.get("speed") != "blitz":
        return Verdict(False, f"speed {head.get('speed')}")
    players = head.get("players") or {}
    white, black = _player(players.get("white") or {}), _player(players.get("black") or {})
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
    if age is not None and age > settings.detection.max_game_age_seconds:
        return Verdict(False, f"started {age:.0f}s ago")
    game = GameInfo(id=head["id"], white=white, black=black, created_at=created_at, plies_at_join=0)
    return Verdict(True, "header ok", game)


async def verify_game(
    client: LichessClient, game_id: str, settings: Settings, now_ms: int | None = None
) -> Verdict:
    """Open the game stream, check the header, then count the replayed plies."""
    now = now_ms if now_ms is not None else int(time.time() * 1000)
    det = settings.detection
    try:
        async with aclosing(client.stream_game(game_id)) as stream:
            try:
                head = await asyncio.wait_for(anext(stream), timeout=det.stream_open_timeout_seconds)
            except StopAsyncIteration:
                return Verdict(False, "empty stream")
            except asyncio.TimeoutError:
                return Verdict(False, "stream open timed out")
            verdict = check_header(head, settings, now)
            if not verdict.accepted:
                return verdict
            game = verdict.game
            assert game is not None

            last_fen: str | None = None
            first_clock_checked = False
            while True:
                try:
                    line = await asyncio.wait_for(anext(stream), timeout=det.replay_idle_seconds)
                except asyncio.TimeoutError:
                    break  # replay burst finished; the game is live from here
                except StopAsyncIteration:
                    return Verdict(False, "stream ended (game over)")
                if "fen" not in line:
                    # A trailing game description means the game just finished.
                    if "id" in line:
                        return Verdict(False, "stream ended (game over)")
                    continue
                last_fen = line["fen"]
                if not first_clock_checked and "wc" in line and "bc" in line:
                    first_clock_checked = True
                    expected = _initial_seconds(det.time_control)
                    if expected is not None and "lm" not in line and (line["wc"] != expected or line["bc"] != expected):
                        return Verdict(False, f"initial clock {line['wc']}+{line['bc']}s is not {expected}s")
                plies = plies_from_fen(last_fen)
                if plies > det.max_start_plies:
                    return Verdict(False, f"{plies} plies already played")
            if last_fen is None:
                return Verdict(False, "no position received")
            game.plies_at_join = plies_from_fen(last_fen)
            game.fen = last_fen
            return Verdict(True, "ok", game)
    except LichessHTTPError as exc:
        return Verdict(False, f"HTTP {exc.status_code}")
    except LichessError as exc:
        return Verdict(False, str(exc))


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
