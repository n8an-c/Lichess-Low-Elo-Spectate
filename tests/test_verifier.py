import asyncio
import json

import httpx
import pytest

from backend.config import DetectionSettings, PoolSettings, Settings
from backend.lichess import LichessClient, LichessSettings
from backend.verifier import Candidates, GameInfo, PlayerInfo, check_header, plies_from_fen, verify_game

START = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1"
NOW = 1_800_000_000_000

SETTINGS = Settings(
    pool=PoolSettings(rating_cap=1000),
    detection=DetectionSettings(
        max_start_plies=10, replay_idle_seconds=0.05, stream_open_timeout_seconds=1.0, time_control="3+0"
    ),
)


@pytest.mark.parametrize(
    "fen,plies",
    [
        (START, 0),
        ("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1", 1),
        ("rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq e6 0 2", 2),
        ("8/8/8/8/8/8/8/8 b - - 0 6", 11),
        ("8/8/8/8/8/8/8/8 w - - 0 6", 10),
    ],
)
def test_plies_from_fen(fen, plies):
    assert plies_from_fen(fen) == plies


def header(**overrides):
    base = {
        "id": "abcd1234",
        "variant": {"key": "standard", "name": "Standard", "short": "Std"},
        "speed": "blitz",
        "perf": "blitz",
        "rated": True,
        "source": "pool",
        "createdAt": NOW - 20_000,
        "players": {
            "white": {"user": {"name": "Alice", "id": "alice"}, "rating": 812},
            "black": {"user": {"name": "Bob", "id": "bob"}, "rating": 947},
        },
    }
    base.update(overrides)
    return base


@pytest.mark.parametrize(
    "overrides,reason",
    [
        ({"rated": False}, "not rated"),
        ({"variant": {"key": "chess960"}}, "variant chess960"),
        ({"speed": "bullet"}, "speed bullet"),
        ({"players": {"white": {"user": {"name": "A", "id": "a"}, "rating": 1000}, "black": {"user": {"name": "B", "id": "b"}, "rating": 500}}}, "A rated 1000 >= 1000"),
        ({"players": {"white": {"user": {"name": "A", "id": "a"}, "rating": 900, "ratingDiff": 5}, "black": {"user": {"name": "B", "id": "b"}, "rating": 500, "ratingDiff": -5}}}, "already finished"),
        ({"players": {"white": {"aiLevel": 1}, "black": {"user": {"name": "B", "id": "b"}, "rating": 500}}}, "anonymous or computer player"),
        ({"createdAt": NOW - 500_000}, "started 500s ago"),
    ],
)
def test_check_header_rejects(overrides, reason):
    v = check_header(header(**overrides), SETTINGS, NOW)
    assert not v.accepted
    assert v.reason == reason


def test_check_header_accepts():
    v = check_header(header(), SETTINGS, NOW)
    assert v.accepted and v.game is not None
    assert v.game.white.name == "Alice" and v.game.black.rating == 947


def test_check_header_rejects_bots_and_provisional_ratings():
    bot = {"white": {"user": {"name": "maia1", "id": "maia1", "title": "BOT"}, "rating": 900},
           "black": {"user": {"name": "B", "id": "b"}, "rating": 500}}
    assert check_header(header(players=bot), SETTINGS, NOW).reason == "maia1 is a bot"
    prov = {"white": {"user": {"name": "A", "id": "a"}, "rating": 900, "provisional": True},
            "black": {"user": {"name": "B", "id": "b"}, "rating": 500}}
    assert check_header(header(players=prov), SETTINGS, NOW).reason == "A has a provisional rating"


def test_rating_rule_either_and_time_control_list():
    either = Settings(pool=PoolSettings(rating_cap=1000),
                      detection=DetectionSettings(rating_rule="either", time_controls=["3+0", "5+3"]))
    mixed = {"white": {"user": {"name": "A", "id": "a"}, "rating": 1300},
             "black": {"user": {"name": "B", "id": "b"}, "rating": 700}}
    assert check_header(header(players=mixed), SETTINGS, NOW).reason == "A rated 1300 >= 1000"
    assert check_header(header(players=mixed), either, NOW).accepted
    strong = {"white": {"user": {"name": "A", "id": "a"}, "rating": 1300},
              "black": {"user": {"name": "B", "id": "b"}, "rating": 1000}}
    assert check_header(header(players=strong), either, NOW).reason == "neither player under 1000 (1300 / 1000)"
    assert either.detection.clock_pairs() == {(180, 0), (300, 3)}
    assert either.detection.speeds() == {"blitz"}  # 5+3 is blitz too (300 + 40*3 = 420 s < 480)
    rapid = Settings(detection=DetectionSettings(time_controls=["10+0"]))
    assert rapid.detection.speeds() == {"rapid"}
    assert check_header(header(), rapid, NOW).reason == "speed blitz"


async def test_verify_accepts_any_configured_initial_clock():
    settings = Settings(pool=PoolSettings(rating_cap=1000),
                        detection=DetectionSettings(time_controls=["3+0", "5+0"], replay_idle_seconds=0.05,
                                                    stream_open_timeout_seconds=1.0, max_start_plies=10))
    lines = move_lines(2)
    lines[0] = {"fen": START, "wc": 300, "bc": 300}
    transport = live_transport(header(), lines)
    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        v = await verify_game(client, "abcd1234", settings, now_ms=NOW)
    assert v.accepted, v.reason
    assert v.game.estimated_plies == 5 and v.game.to_dict()["estimatedPlies"] == 5


def move_lines(n):
    """Initial position followed by n move lines with FENs reflecting the ply count."""
    lines = [{"fen": START, "wc": 180, "bc": 180}]
    for ply in range(1, n + 1):
        side = "b" if ply % 2 else "w"
        fullmove = ply // 2 + 1
        lines.append({"fen": f"8/8/8/8/8/8/8/8 {side} - - 0 {fullmove}", "lm": "a2a3", "wc": 180 - ply, "bc": 180})
    return lines


def live_transport(head, lines, *, finish=False):
    """A transport whose game stream sends the lines then stays open (or closes if finish)."""

    async def body():
        for obj in [head, *lines]:
            yield (json.dumps(obj) + "\n").encode()
        if not finish:
            await asyncio.sleep(5)

    def handler(request):
        if request.url.path.startswith("/api/stream/game/missing"):
            return httpx.Response(404, json={"error": "not found"})
        return httpx.Response(200, content=body(), headers={"content-type": "application/x-ndjson"})

    return httpx.MockTransport(handler)


async def run_verify(transport, game_id="abcd1234"):
    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        return await verify_game(client, game_id, SETTINGS, now_ms=NOW)


async def test_verify_accepts_young_live_game():
    v = await run_verify(live_transport(header(), move_lines(4)))
    assert v.accepted, v.reason
    assert v.game.plies_at_join == 4
    assert v.game.fen.endswith(" w - - 0 3")


async def test_verify_rejects_too_many_plies():
    v = await run_verify(live_transport(header(), move_lines(12)))
    assert not v.accepted
    assert v.reason == "11 plies already played"


async def test_verify_rejects_finished_stream():
    v = await run_verify(live_transport(header(), move_lines(3), finish=True))
    assert not v.accepted
    assert v.reason == "stream ended (game over)"


async def test_verify_rejects_wrong_initial_clock():
    lines = move_lines(2)
    lines[0] = {"fen": START, "wc": 300, "bc": 300}
    v = await run_verify(live_transport(header(), lines))
    assert not v.accepted
    assert v.reason == "initial clock 300+300s is not one of [180]s"


async def test_verify_rejects_header_without_reading_moves():
    v = await run_verify(live_transport(header(rated=False), move_lines(2)))
    assert v.reason == "not rated"


async def test_verify_reports_both_players_to_discover_even_when_rejected():
    seen = []
    strong = {
        "white": {"user": {"name": "Alice", "id": "alice"}, "rating": 2400},
        "black": {"user": {"name": "Bob", "id": "bob"}, "rating": 950},
    }
    transport = live_transport(header(players=strong), move_lines(2))
    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        v = await verify_game(client, "abcd1234", SETTINGS, now_ms=NOW, discover=lambda uid, r: seen.append((uid, r)))
    assert v.reason == "Alice rated 2400 >= 1000"
    assert seen == [("alice", 2400), ("bob", 950)]

    seen.clear()
    transport = live_transport(header(rated=False), move_lines(2))
    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        await verify_game(client, "abcd1234", SETTINGS, now_ms=NOW, discover=lambda uid, r: seen.append((uid, r)))
    assert seen == []  # casual game: ratings are not blitz ratings worth recording


async def test_verify_age_rule_ignores_games_still_at_ply_zero():
    old = header(createdAt=NOW - 500_000)
    v = await run_verify(live_transport(old, move_lines(0)))
    assert v.accepted and v.game.plies_at_join == 0
    v = await run_verify(live_transport(old, move_lines(2)))
    assert not v.accepted and v.reason == "started 500s ago"


async def test_verify_waits_for_a_slow_first_position_line():
    async def body():
        yield (json.dumps(header()) + "\n").encode()
        await asyncio.sleep(0.2)  # longer than replay_idle_seconds (0.05), shorter than the open timeout (1.0)
        for obj in move_lines(2):
            yield (json.dumps(obj) + "\n").encode()
        await asyncio.sleep(5)

    transport = httpx.MockTransport(lambda r: httpx.Response(200, content=body()))
    v = await run_verify(transport)
    assert v.accepted, v.reason
    assert v.game.plies_at_join == 2


async def test_transient_failures_are_marked_for_retry():
    def handler(request):
        raise httpx.ConnectError("boom", request=request)

    v = await run_verify(httpx.MockTransport(handler))
    assert not v.accepted and v.transient
    v = await run_verify(live_transport(header(rated=False), move_lines(2)))
    assert not v.accepted and not v.transient  # a real rejection is final


async def test_verify_handles_404():
    v = await run_verify(live_transport(header(), []), game_id="missing1")
    assert not v.accepted
    assert v.reason == "HTTP 404"


async def test_read_replay_returns_last_line_and_final_description():
    from backend.verifier import StreamEnded, open_game_stream, read_replay

    lines = move_lines(3)
    transport = live_transport(header(), lines)
    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        reader, head = await open_game_stream(client, "abcd1234", SETTINGS.detection)
        assert head["id"] == "abcd1234"
        last = await read_replay(reader, SETTINGS.detection)
        assert last == lines[-1]
        assert last["lm"] == "a2a3" and last["wc"] == 177 and last["bc"] == 180
        await reader.close()

    final = header(status={"id": 31, "name": "resign"}, winner="black")
    transport = live_transport(header(), [*move_lines(2), final], finish=True)
    async with LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=transport) as client:
        reader, _ = await open_game_stream(client, "abcd1234", SETTINGS.detection)
        with pytest.raises(StreamEnded) as exc:
            await read_replay(reader, SETTINGS.detection)
        assert exc.value.description["status"]["name"] == "resign"
        await reader.close()


def game(id_, detected_at):
    p = PlayerInfo("x", "X", 800)
    return GameInfo(id=id_, white=p, black=p, created_at=0, plies_at_join=0, detected_at=detected_at)


def test_candidates_newest_first_and_expiry():
    c = Candidates(ttl_seconds=60)
    c.add(game("a", 100))
    c.add(game("b", 130))
    c.add(game("a", 140))  # re-adding moves it to the front
    assert [g.id for g in c.snapshot()] == ["a", "b"]
    assert [g.id for g in c.expire(now=195)] == ["b"]
    assert c.newest().id == "a"
    assert c.pop_newest().id == "a"
    assert len(c) == 0
