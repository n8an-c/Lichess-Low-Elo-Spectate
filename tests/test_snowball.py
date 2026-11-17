import json

import httpx

from backend.config import DetectionSettings, PoolSettings, Settings
from backend.db import Database, PlayerRow
from backend.lichess import LichessClient, LichessSettings
from backend.snowball import Crawler, Sighting, collect_sightings, qualifies_sighting

NOW = 1_800_000_000_000
DAY = 86_400_000
SETTINGS = Settings(pool=PoolSettings(rating_cap=1000, games_per_request=25, expand_rating_max=950),
                    detection=DetectionSettings(time_controls=["3+0"]))


def game(white, black, *, created=NOW - 2 * DAY, clock=(180, 0), rated=True, variant="standard"):
    """white/black: (id, rating[, extra dict])."""

    def side(p):
        pid, rating, *extra = p
        d = {"user": {"id": pid, "name": pid.capitalize()}, "rating": rating}
        if extra:
            d.update(extra[0])
        return d

    return {
        "id": f"{white[0]}-{black[0]}-{created}",
        "rated": rated,
        "variant": variant,
        "speed": "blitz",
        "createdAt": created,
        "players": {"white": side(white), "black": side(black)},
        "clock": {"initial": clock[0], "increment": clock[1]},
    }


def test_collect_sightings_keeps_matching_games_and_newest_rating():
    docs = [
        game(("alice", 900), ("bob", 950), created=NOW - 3 * DAY),
        game(("bob", 980), ("alice", 905), created=NOW - DAY),  # newer: bob's rating 980 wins
        game(("alice", 900), ("carol", 1200), clock=(300, 0)),  # wrong clock
        game(("alice", 900), ("dave", 700), rated=False),  # casual
        game(("alice", 900), ("erin", 700), variant="chess960"),
        game(("alice", 900), ("maia1", 1500, {"user": {"id": "maia1", "name": "maia1", "title": "BOT"}})),
        game(("alice", 900), ("newbie", 800, {"provisional": True})),
        {"id": "ai", "rated": True, "variant": "standard", "createdAt": NOW - 2 * DAY, "clock": {"initial": 180, "increment": 0},
         "players": {"white": {"user": {"id": "alice", "name": "Alice"}, "rating": 900}, "black": {"aiLevel": 3}}},
    ]
    sightings, own, matching = collect_sightings(docs, {(180, 0)}, parent="Alice")
    assert matching == 5
    assert own is not None and own.games == 5 and own.rating == 905
    assert set(sightings) == {"bob", "maia1", "newbie"}
    assert sightings["bob"].rating == 980 and sightings["bob"].games == 2 and sightings["bob"].last_game_at == NOW - DAY
    assert sightings["maia1"].bot and sightings["newbie"].provisional
    assert qualifies_sighting(sightings["bob"], 1000)
    assert not qualifies_sighting(sightings["maia1"], 1000)
    assert not qualifies_sighting(sightings["newbie"], 1000)
    assert not qualifies_sighting(Sighting("x", "X", 1000, NOW), 1000)


def profile(pid, rating=850, **over):
    base = {"id": pid, "username": pid.capitalize(), "seenAt": NOW - DAY,
            "perfs": {"blitz": {"games": 50, "rating": rating, "rd": 60, "prog": 0}}}
    base.update(over)
    return base


class FakeLichess:
    """Serves games exports, profile look-ups and (optionally) arena seeds."""

    def __init__(self, games_by_user, profiles, arenas=None, arena_results=None):
        self.games_by_user = games_by_user
        self.profiles = profiles
        self.arenas = arenas or {"created": [], "started": [], "finished": []}
        self.arena_results = arena_results or {}
        self.requests = []

    def transport(self):
        def handler(request):
            path = request.url.path
            self.requests.append((request.method, path, dict(request.url.params), request.content.decode()))
            if path.startswith("/api/games/user/"):
                user = path.rsplit("/", 1)[-1]
                assert request.url.params["rated"] == "true" and request.url.params["perfType"]
                assert request.url.params["moves"] == "false" and "since" in request.url.params
                if user not in self.games_by_user:
                    return httpx.Response(404, json={"error": "not found"})
                body = "".join(json.dumps(g) + "\n" for g in self.games_by_user[user])
                return httpx.Response(200, text=body)
            if path == "/api/users" and request.method == "POST":
                ids = request.content.decode().split(",")
                return httpx.Response(200, json=[self.profiles[i] for i in ids if i in self.profiles])
            if path == "/api/tournament":
                return httpx.Response(200, json=self.arenas)
            if path.startswith("/api/tournament/") and path.endswith("/results"):
                arena_id = path.split("/")[3]
                body = "".join(json.dumps(r) + "\n" for r in self.arena_results.get(arena_id, []))
                return httpx.Response(200, text=body)
            return httpx.Response(500, text=f"unexpected {request.method} {path}")

        return httpx.MockTransport(handler)


async def run_crawl(tmp_path, fake, settings=SETTINGS, **kwargs):
    db = Database(tmp_path / "c.sqlite3")
    client = LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=fake.transport())
    crawler = Crawler(client, db, settings)
    kwargs.setdefault("request_budget", 50)
    kwargs.setdefault("since_ms", NOW - 14 * DAY)
    kwargs.setdefault("now_ms", NOW)
    result = await crawler.crawl(**kwargs)
    await client.aclose()
    return result, db


async def test_crawl_expands_from_seeds_confirms_and_drops(tmp_path):
    games = {
        "seed": [game(("seed", 900), ("bob", 950)), game(("carol", 940), ("seed", 902))],
        "bob": [game(("bob", 955), ("dave", 700)), game(("bob", 955), ("strong", 1100))],
        "carol": [game(("carol", 945), ("erin", 800, {"provisional": True}))],
        "dave": [],
    }
    profiles = {
        "seed": profile("seed", 900),
        "bob": profile("bob", 960),
        "carol": profile("carol", 1010),  # drifted above the cap since that game
        "dave": profile("dave", 700, disabled=True),  # closed account
    }
    fake = FakeLichess(games, profiles)
    settings = Settings(pool=PoolSettings(rating_cap=1000, games_per_request=25, seed_players=["Seed"]),
                        detection=DetectionSettings(time_controls=["3+0"]))
    result, db = await run_crawl(tmp_path, fake, settings)
    fetched = [p.rsplit("/", 1)[-1] for m, p, _, _ in fake.requests if p.startswith("/api/games/user/")]
    assert fetched[0] == "seed" and set(fetched) == {"seed", "bob", "carol", "dave"}
    assert result.players_expanded == 4 and result.matching_games == 5
    assert result.candidates == 4  # seed, bob, carol, dave (erin provisional, strong too strong)
    assert result.added == 2 and result.dropped == 2 and result.unconfirmed == 0
    assert sorted(db.active_player_ids()) == ["bob", "seed"]
    assert db.conn.execute("SELECT source_team, game_rating, blitz_rating FROM players WHERE id='bob'").fetchone()[:] == ("games", 955, 960)
    assert db.conn.execute("SELECT active FROM players WHERE id='carol'").fetchone()[0] == 0
    assert db.conn.execute("SELECT expanded_at IS NOT NULL FROM players WHERE id='bob'").fetchone()[0] == 1
    assert result.stopped == "frontier exhausted"
    assert any(m == "POST" and p == "/api/users" for m, p, _, _ in fake.requests)
    db.close()


async def test_crawl_stops_at_budget_and_target(tmp_path):
    games = {f"p{i}": [game((f"p{i}", 900), (f"p{i+1}", 900))] for i in range(20)}
    profiles = {f"p{i}": profile(f"p{i}", 900) for i in range(21)}
    fake = FakeLichess(games, profiles)
    settings = Settings(pool=PoolSettings(rating_cap=1000, games_per_request=25, seed_players=["p0"]),
                        detection=DetectionSettings(time_controls=["3+0"]))
    result, db = await run_crawl(tmp_path, fake, settings, request_budget=3)
    assert result.requests == 3 and "budget" in result.stopped
    db.close()

    fake = FakeLichess(games, profiles)
    (tmp_path / "b").mkdir()
    result, db = await run_crawl(tmp_path / "b", fake, settings, target_members=2)
    assert "target" in result.stopped
    assert db.active_count() >= 2
    db.close()


async def test_trickle_only_fetches_given_parents(tmp_path):
    games = {"act": [game(("act", 900), ("fresh", 850), created=NOW - 3600_000)], "fresh": [game(("fresh", 850), ("other", 800))]}
    profiles = {"act": profile("act", 900), "fresh": profile("fresh", 850), "other": profile("other", 800)}
    fake = FakeLichess(games, profiles)
    db = Database(tmp_path / "t.sqlite3")
    db.upsert_players([PlayerRow("act", "Act", 900, False, NOW, "games", last_game_at=NOW - DAY, game_rating=900)])
    client = LichessClient(LichessSettings(courtesy_delay_seconds=0), transport=fake.transport())
    result = await Crawler(client, db, SETTINGS).crawl(request_budget=5, since_ms=NOW - 7_200_000, parents=["act"], expand=False, now_ms=NOW)
    fetched = [p.rsplit("/", 1)[-1] for m, p, _, _ in fake.requests if p.startswith("/api/games/user/")]
    assert fetched == ["act"]  # "fresh" is added but not expanded
    assert result.added == 1 and sorted(db.active_player_ids()) == ["act", "fresh"]
    assert db.conn.execute("SELECT last_game_at FROM players WHERE id='act'").fetchone()[0] == NOW - 3600_000
    await client.aclose()
    db.close()


async def test_seeds_fall_back_to_capped_arenas(tmp_path):
    arenas = {
        "created": [],
        "started": [{"id": "ar1300", "rated": True, "variant": {"key": "standard"}, "clock": {"limit": 180, "increment": 0},
                     "status": 20, "nbPlayers": 30, "maxRating": {"rating": 1300}}],
        "finished": [{"id": "big", "rated": True, "variant": {"key": "standard"}, "clock": {"limit": 180, "increment": 0},
                      "status": 30, "nbPlayers": 300}],  # uncapped: skipped
    }
    results = {"ar1300": [{"rank": 1, "rating": 1290, "username": "Topdog"}, {"rank": 9, "rating": 940, "username": "LowKey"},
                          {"rank": 12, "rating": 1150, "username": "Mid"}]}
    games = {"lowkey": [game(("lowkey", 940), ("pal", 860))], "mid": [], "pal": []}
    profiles = {"lowkey": profile("lowkey", 940), "pal": profile("pal", 860)}
    fake = FakeLichess(games, profiles, arenas, results)
    result, db = await run_crawl(tmp_path, fake)
    fetched = [p.rsplit("/", 1)[-1] for m, p, _, _ in fake.requests if p.startswith("/api/games/user/")]
    assert fetched[0] == "lowkey"  # lowest-rated arena participant first; Topdog (1290) is over cap+300? no: 1290 < 1300 -> included after
    assert "topdog" not in fetched[:1]
    assert sorted(db.active_player_ids()) == ["lowkey", "pal"]
    assert result.added == 2
    db.close()


async def test_crawl_requests_the_perf_types_of_the_configured_clocks(tmp_path):
    fake = FakeLichess({"seed": []}, {})
    settings = Settings(pool=PoolSettings(rating_cap=1000, seed_players=["seed"]),
                        detection=DetectionSettings(time_controls=["3+0", "10+0"]))
    await run_crawl(tmp_path, fake, settings)
    games_requests = [params for m, p, params, _ in fake.requests if p.startswith("/api/games/user/")]
    assert games_requests and games_requests[0]["perfType"] == "blitz,rapid"
    assert games_requests[0]["max"] == "75"


async def test_crawl_without_any_seed_reports_it(tmp_path):
    fake = FakeLichess({}, {})
    result, db = await run_crawl(tmp_path, fake)
    assert result.stopped == "no seeds" and result.requests == 0
    db.close()
