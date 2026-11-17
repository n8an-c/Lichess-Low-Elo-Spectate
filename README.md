# Low-ELO Lichess Spectator

A local web app that finds rated 3+0 blitz games on Lichess where both players
are rated under 1000 and lists them, as they start, with links to watch them
live on lichess.org. See `SPEC.md` for the original design.

## Setup

Python 3.11+ is required. There is no frontend build step.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## Run

```bash
python -m backend.cli serve
```

Open http://127.0.0.1:8000/. On the first start the backend builds the player
pool in the background (a few minutes, see below) while it already polls the
members it has; games appear on the page as soon as one qualifies. The page
lists every qualifying game found, newest first, with both players and
ratings, the ply it was found at, how long ago it started, and a **Watch on
Lichess** link. It refreshes every few seconds by polling the backend.

To build or top up the pool without serving (prints a summary):

```bash
python -m backend.cli build-pool            # uses [pool] from config.toml
python -m backend.cli build-pool --max-members 500 --budget 60   # quick trial
python -m backend.cli show-pool --limit 30
```

## How the pool is built

`[pool] source` in `config.toml`:

- **`games` (default): snowball from rated games.** Starting from a few seed
  players (`seed_players`, else unexpanded pool members, else the low-rated
  participants of Lichess's rating-capped arenas), the crawler fetches each
  player's recent rated games (`GET /api/games/user/{id}`, 75 per request),
  keeps the opponents who were rated under the cap in games with the
  configured time controls, and expands from them, lowest-rated and most
  recently active first. Every new member is confirmed through
  `POST /api/users` (closed accounts, bots, provisional ratings and players
  who have drifted above the cap are dropped). Measured: 600 proven 3+0
  players in under two minutes, against 878 members in 47 minutes from
  teams, most of whom never played a rated 3+0 game. The crawl stops at
  `max_members` or after `crawl_request_budget` requests. While serving, a
  **trickle** re-reads the latest games of the most active and currently
  online members every `trickle_interval_minutes` to pick up opponents who
  are likely playing right now, and a **light refresh** every
  `light_refresh_minutes` re-checks stale profiles, looks up opponents seen in
  game headers, and expires members without a game in `games_since_days`.
- **`teams`: scan team rosters** (`team_ids` plus optional `team_keywords`).
  Slow (Lichess streams ~20 profiles/s and caps each team at its 1,000 newest
  members, about 50 s per team) and low-yield, kept for reference.
- **`both`**: teams first, then the crawl tops the pool up.

## How games are found

Two detectors feed the same verifier; `[detection] mode` picks `"poll"`
(default), `"stream"` or `"both"`:

- **Polling** asks `/api/users/status?withGameMetas=true` about pooled
  players, 100 per request, in two tiers: players reported online are
  re-polled every `hot_poll_seconds`; everyone else is swept
  `cold_batches_per_tick` batches per tick and joins the hot tier the moment
  they show up online. A game by an online member is noticed within a few
  seconds whatever the pool size; requests stay at roughly one per second.
- **Streaming** keeps `POST /api/stream/games-by-users` connections open
  (one per 300 members). Lichess only reports a game when **both players are
  in the same list**, so with a normal pool this detector sees almost nothing;
  it is off by default.

Every game found is **verified** by opening its stream (up to
`verify_concurrency` at once): rated, standard, an allowed time control,
the rating rule, no bots or provisional ratings, not finished, and at most
`max_start_plies` replayed plies. Lichess delays spectator streams by about
3 plies, so the page shows an estimated real ply (replayed + 3). Both
detectors share one pending set and the `seen_games` table, so a game is
verified once.

Rules you can relax in `[detection]`: `time_controls` (e.g. `["3+0", "3+2",
"5+0", "5+3"]`, roughly 3x the games), `rating_rule = "either"` (at least one
player under the cap, about 2x), and `max_start_plies`.

Log lines to look for:

```
crawl [12/300] somebody: 75 games, 61 matching, 48 opponents, 23 new candidates (pool 412, frontier 318)
found 3+0 game AbCdEfGh (https://lichess.org/AbCdEfGh); verifying
ACCEPTED AbCdEfGh: alice (812) vs bob (947), 4 plies  https://lichess.org/AbCdEfGh
rejected AbCdEfGh: 24 plies already played
poll pass 12: 2014 players, 41 online (hot), 9 playing, 3 in 3+0 games, 1 queued, sweep 24s
```

## API (local, no auth)

- `GET /api/games?limit=50`: recent accepted games (newest first) with `url`,
  players, `pliesAtJoin`, `estimatedPlies`, `createdAt`, `foundAt`,
  `ageSeconds`, plus `stats` (pool size and source, online members, detection
  mode and rules, accepted/rejected counts, requests per minute, rate-limit
  pauses, last crawl summary, uptime).
- `GET /api/candidates`: accepted games still within `candidate_ttl_seconds`.

## Tests

```bash
pytest
```

## Lichess API notes (verified 2026-11-04)

- Lichess asks for one request at a time; the client serialises requests
  through one lock, spaces them by `courtesy_delay_seconds`, and pauses
  everything for `Retry-After` or 60 s on HTTP 429.
- `GET /api/games/user/{username}` streams 20 games/s anonymously (30/s with
  a token) and rounds `max` above 25 up to a multiple of 25; `players.*.rating`
  is the rating at the time of that game.
- `GET /api/users/status?withGameMetas=true` reports `online` and
  `playing.clock` (e.g. `"3+0"`); it is cheap and may be called every few seconds.
- `POST /api/users` takes 300 IDs per call, 8,000 users per 10 minutes.
- `GET /api/team/{id}/users?full=true` returns full profiles for the 1,000
  newest members at ~20 profiles/s.
- `GET /api/stream/games-by-users` only reports games where both players
  are in the submitted list.
- `GET /api/stream/game/{id}` has no clock field in its first line, replays
  all moves so far before going live, delays ongoing games by 3 moves, and
  allows at most 8 open streams per IP.
- Rating-capped arenas (`≤1300 SuperBlitz` and friends) exist on Lichess's
  hourly schedule; their results (`GET /api/tournament/{id}/results`) are
  used only as crawl seeds, since few participants are rated under 1000.
