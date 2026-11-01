# Low-ELO Lichess Spectator

A local web app that finds rated 3+0 blitz games on Lichess where both players
are rated under 1000 and shows them live. See `SPEC.md` for the design.

Current state: **milestone 1** (rate-limited Lichess client + pool builder).

## Setup

Python 3.11+ is required.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

## Build the player pool

```bash
python -m backend.cli build-pool
python -m backend.cli show-pool --limit 30
```

`build-pool` reads `config.toml`, finds teams (configured IDs plus keyword
search), streams each team's most recent members with their ratings, and
writes players with a non-provisional blitz rating under the cap who were
seen in the last 7 days to `spectate.sqlite3`.

Lichess streams team members at roughly 20 profiles per second, and only
about 0.5% of members of a typical "beginner" team pass the filter, so a
full build with the default config (about 60 teams, up to 1,500 members
each) can take 30 to 60 minutes. Players are saved after every team, so you
can stop a run with Ctrl+C and still have a partial pool. For a quick trial:

```bash
python -m backend.cli build-pool --max-teams 5
```

Useful flags:

- `--max-teams N` scans only the first N teams (configured teams come first).
- `--log-level DEBUG` logs every Lichess request.
- `--config path/to/other.toml` uses a different config file.

Inspect the database directly if you prefer:

```bash
sqlite3 spectate.sqlite3 "select count(*) from players where active = 1"
```

## Tests

```bash
pytest
```

## Lichess API notes (verified 2026-11-01)

- Lichess asks for one request at a time; the client serialises all requests
  through one lock, waits a courtesy delay after each, and pauses everything
  for 60 s on HTTP 429.
- `GET /api/team/{id}/users?full=true` returns full profiles but only the
  1,000 most recent members. Previously pooled players that fall out of that
  window are re-checked with `POST /api/users` (300 IDs per call, 8,000 users
  per 10 minutes allowed).
- `GET /api/users/status?withGameMetas=true` reports `playing.clock` (e.g.
  `"3+0"`), which the milestone 2 poller will use to pre-filter games.
- `GET /api/stream/game/{id}` has no clock field in its first line, delays
  ongoing games by 3 moves, and allows at most 8 open streams per IP.
