import asyncio
import json

import httpx
import pytest

from backend.config import DetectionSettings, PoolSettings, Settings
from backend.lichess import LichessClient, LichessSettings
from backend.verifier import Candidates, GameInfo, PlayerInfo
from backend.viewer import Broadcaster, Viewer

START = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"
SETTINGS = Settings(
    pool=PoolSettings(rating_cap=1000),
    detection=DetectionSettings(
        max_start_plies=10, replay_idle_seconds=0.05, stream_open_timeout_seconds=1.0, time_control="3+0"
    ),
)


def header(game_id, white=812, black=947, **extra):
    h = {
        "id": game_id,
        "variant": {"key": "standard", "name": "Standard", "short": "Std"},
        "speed": "blitz",
        "perf": "blitz",
        "rated": True,
        "createdAt": 1_800_000_000_000,
        "players": {
            "white": {"user": {"name": "Alice", "id": "alice"}, "rating": white},
            "black": {"user": {"name": "Bob", "id": "bob"}, "rating": black},
        },
    }
    h.update(extra)
    return h


def move_lines(n):
    lines = [{"fen": START, "wc": 180, "bc": 180}]
    for ply in range(1, n + 1):
        side = "b" if ply % 2 else "w"
        lines.append({"fen": f"8/8/8/8/8/8/8/8 {side} - - 0 {ply // 2 + 1}", "lm": "a2a3", "wc": 180 - ply, "bc": 180})
    return lines


class ScriptedLichess:
    """Game streams: replay lines, then live lines released one by one via `release`."""

    def __init__(self):
        self.games: dict[str, dict] = {}
        self.events: dict[str, asyncio.Queue] = {}

    def add(self, game_id, head, replay):
        self.games[game_id] = {"head": head, "replay": replay}
        self.events[game_id] = asyncio.Queue()

    async def send(self, game_id, obj):
        await self.events[game_id].put(obj)

    async def end(self, game_id):
        await self.events[game_id].put(None)

    def transport(self):
        async def body(game_id):
            g = self.games[game_id]
            for obj in [g["head"], *g["replay"]]:
                yield (json.dumps(obj) + "\n").encode()
            while True:
                obj = await self.events[game_id].get()
                if obj is None:
                    return
                yield (json.dumps(obj) + "\n").encode()

        def handler(request):
            game_id = request.url.path.rsplit("/", 1)[-1]
            if game_id not in self.games:
                return httpx.Response(404, json={"error": "not found"})
            return httpx.Response(200, content=body(game_id))

        return httpx.MockTransport(handler)


def candidate(game_id):
    p = PlayerInfo("alice", "Alice", 812)
    return GameInfo(id=game_id, white=p, black=p, created_at=0, plies_at_join=2)


class Collector:
    def __init__(self, broadcaster):
        self.events = []
        self.task = asyncio.create_task(self._run(broadcaster))

    async def _run(self, broadcaster):
        async for event, data in broadcaster.subscribe():
            self.events.append((event, data))

    async def wait_for(self, name, count=1, timeout=2):
        async def _wait():
            while sum(1 for e, _ in self.events if e == name) < count:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(_wait(), timeout)


@pytest.fixture
async def rig():
    lichess = ScriptedLichess()
    client = LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=lichess.transport())
    broadcaster = Broadcaster()
    candidates = Candidates(ttl_seconds=60)
    viewer = Viewer(client, candidates, SETTINGS, broadcaster)
    collector = Collector(broadcaster)
    task = asyncio.create_task(viewer.run())
    yield lichess, viewer, candidates, collector
    task.cancel()
    collector.task.cancel()
    await viewer.stop()
    await client.aclose()


async def test_viewer_follows_game_to_the_end(rig):
    lichess, viewer, candidates, collector = rig
    lichess.add("game0001", header("game0001"), move_lines(4))
    candidates.add(candidate("game0001"))
    viewer.notify_candidate(candidates.newest())

    await collector.wait_for("game")
    assert viewer.state.status == "live"
    assert viewer.state.plies == 4
    assert viewer.state.game.white.name == "Alice"
    # Clocks and last move are seeded from the replay's last line.
    joined = collector.events[-1][1]
    assert (joined["wc"], joined["bc"], joined["lastMove"]) == (176, 180, "a2a3")
    assert joined["clockAt"] is not None and joined["waiting"] is None

    await lichess.send("game0001", {"fen": "8/8/8/8/8/8/8/8 b - - 0 3", "lm": "e2e4", "wc": 170, "bc": 175})
    await lichess.send("game0001", {"fen": "8/8/8/8/8/8/8/8 w - - 0 4", "lm": "e7e5", "wc": 170, "bc": 171})
    await collector.wait_for("move", 2)
    assert viewer.state.plies == 6
    assert viewer.state.last_move == "e7e5"
    assert viewer.state.wc == 170 and viewer.state.bc == 171
    move_event = [d for e, d in collector.events if e == "move"][-1]
    assert move_event["clockAt"] == viewer.state.clock_at
    clock_at = viewer.state.clock_at

    final = header("game0001", fen="8/8/8/8/8/8/8/8 w - - 0 4", turns=6, status={"id": 30, "name": "mate"}, winner="black")
    await lichess.send("game0001", final)
    await lichess.end("game0001")
    await collector.wait_for("end")
    assert viewer.state.status == "finished"
    assert viewer.state.result == {"status": "mate", "winner": "black"}
    assert viewer.state.last_move == "e7e5"  # final description has no lastMove; keep the highlight
    assert viewer.state.clock_at == clock_at  # clocks frozen at the last move
    await asyncio.sleep(0.1)
    assert viewer.state.status == "finished"  # stays until next()

    snapshot = await viewer.next()
    assert snapshot["status"] == "waiting"
    assert snapshot["waiting"] == {"candidates": 0, "poolSize": None}
    await collector.wait_for("waiting")


async def test_viewer_force_watch_finished_game_shows_result(rig):
    lichess, viewer, candidates, collector = rig
    final = header("over00001", fen="8/8/8/8/8/8/8/8 w - - 0 10", turns=18, status={"id": 31, "name": "resign"}, winner="black")
    lichess.add("over00001", header("over00001"), [*move_lines(18), final])
    await lichess.end("over00001")
    await viewer.watch("over00001", force=True)
    await collector.wait_for("end")
    assert viewer.state.status == "finished"
    assert viewer.state.result == {"status": "resign", "winner": "black"}
    assert viewer.state.plies == 18
    assert (viewer.state.wc, viewer.state.bc, viewer.state.last_move) == (162, 180, "a2a3")
    assert [e for e, _ in collector.events][-2:] == ["game", "end"]


async def test_viewer_candidate_finished_game_is_skipped(rig):
    lichess, viewer, candidates, collector = rig
    final = header("over00002", status={"id": 31, "name": "resign"}, winner="white")
    lichess.add("over00002", header("over00002"), [*move_lines(2), final])
    await lichess.end("over00002")
    candidates.add(candidate("over00002"))
    viewer.notify_candidate(candidates.newest())
    await asyncio.sleep(0.3)
    assert viewer.state.status == "waiting"
    assert len(candidates) == 0


async def test_viewer_skips_candidates_past_the_late_join_limit(rig):
    lichess, viewer, candidates, collector = rig
    lichess.add("toolate01", header("toolate01"), move_lines(12))
    lichess.add("fresh0001", header("fresh0001"), move_lines(2))
    candidates.add(candidate("fresh0001"))
    candidates.add(candidate("toolate01"))  # newest first, so tried first
    viewer.notify_candidate(candidates.newest())

    await collector.wait_for("game")
    assert viewer.state.game.id == "fresh0001"
    assert viewer.state.plies == 2
    assert len(candidates) == 0


async def test_viewer_watch_force_ignores_rating(rig):
    lichess, viewer, candidates, collector = rig
    lichess.add("strong001", header("strong001", white=2400, black=2500), move_lines(30))
    await viewer.watch("strong001", force=True)
    await collector.wait_for("game")
    assert viewer.state.game.id == "strong001"
    assert viewer.state.plies == 30


async def test_viewer_unknown_game_keeps_waiting(rig):
    lichess, viewer, candidates, collector = rig
    await viewer.watch("missing01")
    await asyncio.sleep(0.2)
    assert viewer.state.status == "waiting"


async def test_broadcaster_fanout_and_slow_subscriber_drop():
    b = Broadcaster(max_queue=2)
    got_a, got_b = [], []

    async def consume(store, delay):
        async for item in b.subscribe():
            await asyncio.sleep(delay)
            store.append(item)

    ta = asyncio.create_task(consume(got_a, 0))
    tb = asyncio.create_task(consume(got_b, 10))  # never drains: gets dropped
    await asyncio.sleep(0.01)
    for i in range(4):
        b.publish("move", i)
        await asyncio.sleep(0.01)  # the fast consumer drains between publishes; the slow one cannot
    await asyncio.sleep(0.05)
    assert [d for _, d in got_a] == [0, 1, 2, 3]
    assert b.subscriber_count == 1
    ta.cancel()
    tb.cancel()
