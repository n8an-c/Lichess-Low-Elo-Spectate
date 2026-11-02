# Low-ELO Lichess Spectator

A local web app that finds rated 3+0 blitz games on Lichess where both players
are rated under 1000 and shows them live. See `SPEC.md` for the design.

Current state: **milestone 3** (pool builder, poller, verifier, SSE API and a live chessground board).

## Setup

Python 3.11+ and Node 20+ (for the frontend build) are required.

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

## Watch for games (milestone 2)

```bash
python -m backend.cli watch
```

`watch` runs three loops in one process:

- **Pool refresh**: rebuilds the pool on startup if it is empty or older
  than `pool_refresh_hours`, then every `pool_refresh_hours`. Polling keeps
  using the existing pool while a rebuild runs. Pass `--skip-pool-build` to
  poll the current database without any rebuild (handy while developing,
  since a full build takes a while).
- **Poller**: asks `/api/users/status` about 100 pooled players at a time
  with `withGameMetas=true`, and queues every unseen `3+0` standard game.
- **Verifier**: opens the game stream, checks rated / standard / blitz /
  both ratings under the cap / not finished / started recently, reads the
  replayed moves and accepts the game if it is at most `max_start_plies` in.

Log lines to look for:

```
found 3+0 game AbCdEfGh (https://lichess.org/AbCdEfGh); verifying
ACCEPTED AbCdEfGh: alice (812) vs bob (947), 4 plies  https://lichess.org/AbCdEfGh
rejected AbCdEfGh: 14 plies already played
poll pass 12: 164 players, 9 online, 3 playing, 1 in 3+0 games, 0 queued
```

Accepted games are kept in memory for `candidate_ttl_seconds` (newest first)
for the viewer in milestone 3. Every verified game ID is stored in
`seen_games` so it is never checked twice.

## Run the web app (milestone 3)

Build the frontend once (and again after changing anything under `frontend/`):

```bash
cd frontend
npm install
npm run build
cd ..
```

Then start the backend, which also serves the built frontend:

```bash
python -m backend.cli serve
```

Open http://127.0.0.1:8000/. The page shows "Waiting for a qualifying game"
until the poller finds one, then the board follows it live. The "Next game"
button drops the current game and shows the newest candidate.

During frontend development run `npm run dev` inside `frontend/` as well and
open http://127.0.0.1:5173/ instead; Vite proxies `/api` to the backend.

API (all local, no auth):

- `GET /api/state`: the current viewer snapshot.
- `GET /api/events`: server-sent events. First event is `state`; then `game`
  (a new game is being shown), `move` (`fen`, `lastMove`, `plies`, `wc`, `bc`),
  `end` (final state with `result`) and `waiting`.
- `POST /api/next`: drop the current game and pick the next candidate.
- `POST /api/watch/{gameId}?force=true`: debug aid that follows any live game
  regardless of the rating rules, e.g. a Lichess TV game, to try the UI.
- `GET /api/candidates`: the accepted games waiting to be shown.

Lichess delays live game streams by 3 moves, so the board (and the ply
count) runs slightly behind the real game.

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
