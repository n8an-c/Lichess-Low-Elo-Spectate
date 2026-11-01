# Low-ELO Lichess Spectator: Spec

## Goal
A local web app that finds rated 3+0 blitz games on Lichess where both
players are rated under 1000, and shows them live from (nearly) the first move.

## Decisions

| Area | Decision |
|---|---|
| Platform | Web app, run locally, single user (no auth, no hosting) |
| Watch mode | One game at a time |
| Time control | 3+0 blitz only (clock: 180s initial, 0 increment) |
| Rated/variant | Rated, standard chess only |
| Rating rule | Both players' blitz rating < 1000 |
| Provisional | Exclude players whose blitz rating is provisional |
| Pool source | Lichess improvement/beginner teams: config list of team IDs + keyword discovery |
| Pool refresh | Every few hours (configurable, default 3h) |
| Storage | SQLite |
| Detection | Status polling, 100 user IDs per request |
| Selection | Newest qualifying game first |
| Late joins | Show only if the game is at most ~5 moves in (≤10 plies) |
| Backend | Local Python: FastAPI + httpx (async) |
| Transport | Server-sent events (backend → browser) |
| Board | chessground |
| Display | Board, live clocks, eval bar (toggleable) |
| Engine | Stockfish WASM in the browser, modest depth so it keeps up with 3+0 |
| Game end | Final position stays until the user clicks "Next game" |
| Extras | None for v1 |

## Lichess API endpoints (verify exact params/fields against https://lichess.org/api)
- `GET /api/team/search?text=...`: find teams by keyword
- `GET /api/team/{teamId}/users`: team members, NDJSON stream
- `POST /api/users`: body = comma-separated IDs (max 300) → profiles with `perfs.blitz.rating`, `perfs.blitz.prov`, `seenAt`
- `GET /api/users/status?ids=...&withGameIds=true`: up to 100 IDs → online/playing status and current game ID
- `GET /api/stream/game/{id}`: NDJSON; first line is game metadata (players, ratings, clock, rated, speed, ply count), then one line per move (FEN, last move, clocks)

## Rate limiting
- One request at a time to Lichess, through a single global queue.
- On HTTP 429: pause all requests for a full 60 seconds.
- Small courtesy delay between batch requests.

## Pipeline

### 1. Pool builder (runs on startup, then every few hours)
1. Collect team IDs: config list + keyword search ("beginner", "improvement", etc.).
2. Stream members of each team; dedupe.
3. Batch-fetch profiles (300 per call).
4. Keep users where: blitz rating < 1000, not provisional, seen in the last 7 days.
5. Upsert into SQLite `players`; mark users no longer qualifying as inactive.

### 2. Poller (runs continuously)
1. Loop through active pool players in batches of 100.
2. For any player newly marked as playing, with a game ID not seen before: queue for verification.
3. Poll interval: a few seconds per batch, respecting the global queue.

### 3. Verifier
1. Open `/api/stream/game/{id}` and read the first line.
2. Accept only if: rated, standard variant, clock 180+0, both ratings < 1000, ply count ≤ 10.
3. Add accepted games to an in-memory candidate list (newest first) and record the game ID in SQLite so it's never re-checked.
4. Expire candidates once they pass ~10 plies or end.

### 4. Viewer
- On "Next game" (and on initial load), the backend picks the newest candidate still within the late-join limit and streams its moves to the browser over SSE.
- If none qualify, wait and send the next one as soon as it's verified.
- Browser renders with chessground, runs live clocks between move updates, and feeds each FEN to Stockfish for the eval bar.
- When the game ends, show the result and final position; wait for the user to click "Next game".

## SQLite schema (starting point)
- `teams(id TEXT PK, name TEXT, source TEXT, last_scanned INTEGER)`
- `players(id TEXT PK, blitz_rating INTEGER, provisional INTEGER, seen_at INTEGER, active INTEGER, updated_at INTEGER)`
- `seen_games(id TEXT PK, first_seen INTEGER, accepted INTEGER)`

## Config (e.g. `config.toml`)
- `team_ids` (list), `team_keywords` (list)
- `rating_cap = 1000`, `max_start_plies = 10`
- `pool_refresh_hours = 3`, `active_days = 7`
- `poll_interval_seconds`, `engine_depth`

## Suggested layout
```
backend/
  main.py          # FastAPI app, SSE endpoints
  lichess.py       # rate-limited client (global queue, 429 handling)
  pool.py          # pool builder
  poller.py        # status polling
  verifier.py      # game metadata checks
  db.py            # SQLite access
frontend/
  index.html
  src/main.ts      # SSE client, chessground, clocks
  src/engine.ts    # Stockfish WASM wrapper, eval bar
config.toml
```

## Milestones
1. Rate-limited Lichess client + pool builder writing to SQLite.
2. Poller + verifier logging qualifying games to the console.
3. SSE endpoint + frontend board showing a live game.
4. Clocks, "Next game" button, end-of-game handling.
5. Stockfish eval bar with toggle.