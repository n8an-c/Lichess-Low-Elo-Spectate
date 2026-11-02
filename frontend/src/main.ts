import { Chessground } from "chessground";
import type { Api } from "chessground/api";
import type { Key } from "chessground/types";
import "./style.css";
import { clockRunning, describeResult, formatClock, remainingSeconds, sideToMove, type Color } from "./clock";
import type { MoveEvent, ViewerState } from "./types";

const boardEl = document.getElementById("board")!;
const statusEl = document.getElementById("status")!;
const resultEl = document.getElementById("result")!;
const linkEl = document.getElementById("link")!;
const nextBtn = document.getElementById("next") as HTMLButtonElement;
const clockEls: Record<Color, HTMLElement> = {
  white: document.getElementById("clock-white")!,
  black: document.getElementById("clock-black")!,
};
const playerEls: Record<Color, HTMLElement> = {
  white: document.getElementById("white")!,
  black: document.getElementById("black")!,
};

const START_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1";
const LOW_TIME_SECONDS = 10;

const cg: Api = Chessground(boardEl, {
  fen: START_FEN,
  orientation: "white",
  viewOnly: true,
  coordinates: true,
  animation: { duration: 150 },
  highlight: { lastMove: true, check: true },
});

let current: ViewerState | null = null;
let connected = false;
let nextPending = false;

// -- board ---------------------------------------------------------------------

function uciToKeys(uci: string | null): Key[] | undefined {
  if (!uci || uci.length < 4) return undefined;
  return [uci.slice(0, 2) as Key, uci.slice(2, 4) as Key];
}

function applyPosition(fen: string | null, lastMove: string | null) {
  const f = fen ?? START_FEN;
  cg.set({ fen: f, lastMove: uciToKeys(lastMove), turnColor: sideToMove(f) });
}

// -- players, clocks -----------------------------------------------------------

function setPlayer(color: Color, state: ViewerState) {
  const el = playerEls[color];
  const p = state.game?.[color];
  el.querySelector(".name")!.textContent = p ? (p.title ? `${p.title} ${p.name}` : p.name) : "—";
  el.querySelector(".rating")!.textContent = p ? String(p.rating) : "";
  el.classList.toggle("winner", state.status === "finished" && state.result?.winner === color);
}

function renderClocks() {
  const state = current;
  if (!state || state.status === "waiting") {
    for (const c of ["white", "black"] as Color[]) {
      clockEls[c].textContent = "–:––";
      clockEls[c].classList.remove("running", "low");
    }
    return;
  }
  const running = connected && clockRunning(state);
  const toMove = sideToMove(state.fen);
  const now = Date.now();
  for (const c of ["white", "black"] as Color[]) {
    const isRunning = running && toMove === c;
    const secs = remainingSeconds(c === "white" ? state.wc : state.bc, state.clockAt, now, isRunning);
    const el = clockEls[c];
    el.textContent = formatClock(secs);
    el.classList.toggle("running", state.status === "live" && toMove === c);
    el.classList.toggle("low", secs !== null && secs < LOW_TIME_SECONDS && state.status === "live");
  }
}

setInterval(renderClocks, 100);

// -- status, button ------------------------------------------------------------

function renderButton(state: ViewerState | null) {
  if (!state || state.status === "waiting") {
    nextBtn.textContent = "Waiting…";
    nextBtn.disabled = true;
  } else {
    nextBtn.textContent = state.status === "live" ? "Skip game" : "Next game";
    nextBtn.disabled = nextPending;
  }
}

function render(state: ViewerState) {
  current = state;
  nextPending = false;
  setPlayer("white", state);
  setPlayer("black", state);
  applyPosition(state.fen, state.lastMove);
  resultEl.hidden = state.status !== "finished";
  if (state.status === "waiting") {
    const w = state.waiting;
    const detail = w ? ` · polling ${w.poolSize ?? "?"} players, ${w.candidates} candidate${w.candidates === 1 ? "" : "s"}` : "";
    statusEl.textContent = `Waiting for a qualifying game…${detail}`;
    linkEl.textContent = "";
  } else if (state.status === "live") {
    statusEl.textContent = `Live · ply ${state.plies}`;
  } else {
    resultEl.textContent = describeResult(state.result);
    statusEl.textContent = `Final position · ${state.plies} plies · click Next game to continue`;
  }
  if (state.game) {
    linkEl.innerHTML = "";
    const a = document.createElement("a");
    a.href = state.game.url;
    a.target = "_blank";
    a.rel = "noopener";
    a.textContent = `lichess.org/${state.game.id}`;
    linkEl.appendChild(a);
  }
  document.body.dataset.status = state.status;
  renderButton(state);
  renderClocks();
}

function onMove(move: MoveEvent) {
  if (!current) return;
  current = { ...current, ...move };
  applyPosition(move.fen, move.lastMove);
  statusEl.textContent = `Live · ply ${move.plies}`;
  renderClocks();
}

// -- SSE -----------------------------------------------------------------------

function parse<T>(e: Event): T {
  return JSON.parse((e as MessageEvent).data) as T;
}

function connect() {
  const es = new EventSource("/api/events");
  es.onopen = () => {
    connected = true;
  };
  for (const name of ["state", "game", "waiting", "end"]) {
    es.addEventListener(name, (e) => {
      connected = true;
      render(parse<ViewerState>(e));
    });
  }
  es.addEventListener("move", (e) => onMove(parse<MoveEvent>(e)));
  es.onerror = () => {
    // EventSource reconnects on its own; the first event afterwards is a full `state`.
    connected = false;
    statusEl.textContent = "Connection lost, reconnecting…";
    renderClocks();
  };
}

// -- next game -----------------------------------------------------------------

async function requestNext() {
  if (nextPending || !current || current.status === "waiting") return;
  nextPending = true;
  renderButton(current);
  try {
    await fetch("/api/next", { method: "POST" });
  } catch {
    nextPending = false;
    renderButton(current);
  }
  // The `waiting` / `game` event that follows re-enables the button.
}

nextBtn.addEventListener("click", requestNext);
document.addEventListener("keydown", (e) => {
  if ((e.key === "n" || e.key === "N") && !e.metaKey && !e.ctrlKey && !e.altKey) requestNext();
});

connect();
