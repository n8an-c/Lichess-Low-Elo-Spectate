import { Chessground } from "chessground";
import type { Api } from "chessground/api";
import type { Key } from "chessground/types";
import "./style.css";
import { clockRunning, describeResult, formatClock, remainingSeconds, sideToMove, type Color } from "./clock";
import { Engine, type EvalUpdate } from "./engine";
import { formatEval, whiteShare } from "./evalmath";
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

const evalBarEl = document.getElementById("eval-bar")!;
const evalWhiteEl = evalBarEl.querySelector(".eval-white") as HTMLElement;
const evalLabelEl = document.getElementById("eval-label")!;
const evalDepthEl = document.getElementById("eval-depth")!;
const evalToggleBtn = document.getElementById("eval-toggle") as HTMLButtonElement;

const START_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1";
const LOW_TIME_SECONDS = 10;
const DEFAULT_DEPTH = 12;
const EVAL_PREF_KEY = "evalBar";

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

// -- eval bar / engine ---------------------------------------------------------

let evalEnabled = (() => {
  try {
    return localStorage.getItem(EVAL_PREF_KEY) !== "off";
  } catch {
    return true;
  }
})();

const engine = new Engine(DEFAULT_DEPTH, {
  onReady: () => {
    document.body.dataset.engine = "ready";
    if (current?.fen && current.status !== "waiting") engine.evaluate(current.fen);
  },
  onEval: showEval,
  onError: (msg) => {
    console.error(msg);
    document.body.dataset.engine = "error";
    evalLabelEl.textContent = "n/a";
    evalDepthEl.textContent = "engine unavailable";
  },
});

function showEval(update: EvalUpdate) {
  if (!current || update.fen !== current.fen) return; // stale result for an older position
  const blackToMove = sideToMove(update.fen) === "black";
  const share = whiteShare(update, blackToMove);
  evalWhiteEl.style.height = `${(share * 100).toFixed(1)}%`;
  evalLabelEl.textContent = formatEval(update);
  evalBarEl.classList.toggle("black-ahead", share < 0.5);
  evalDepthEl.textContent = `d${update.depth}`;
}

function clearEval() {
  evalWhiteEl.style.height = "50%";
  evalLabelEl.textContent = "";
  evalDepthEl.textContent = "";
  evalBarEl.classList.remove("black-ahead");
}

function requestEval() {
  if (!evalEnabled || !current) return;
  if (current.status === "waiting" || !current.fen) {
    clearEval();
    return;
  }
  engine.evaluate(current.fen);
}

function applyEvalPreference() {
  document.body.dataset.eval = evalEnabled ? "on" : "off";
  evalToggleBtn.textContent = `Eval: ${evalEnabled ? "on" : "off"}`;
  evalBarEl.hidden = !evalEnabled;
  if (evalEnabled) {
    document.body.dataset.engine = "loading";
    engine.start();
    requestEval();
  } else {
    engine.stop();
    document.body.dataset.engine = "off";
    clearEval();
  }
}

function toggleEval() {
  evalEnabled = !evalEnabled;
  try {
    localStorage.setItem(EVAL_PREF_KEY, evalEnabled ? "on" : "off");
  } catch {
    // private mode etc.: preference just does not persist
  }
  applyEvalPreference();
}

evalToggleBtn.addEventListener("click", toggleEval);

async function loadConfig() {
  try {
    const r = await fetch("/api/config");
    const cfg = (await r.json()) as { engineDepth?: number };
    if (cfg.engineDepth && cfg.engineDepth > 0) engine.setDepth(cfg.engineDepth);
  } catch {
    // keep the default depth
  }
}

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
  requestEval();
}

function onMove(move: MoveEvent) {
  if (!current) return;
  current = { ...current, ...move };
  applyPosition(move.fen, move.lastMove);
  statusEl.textContent = `Live · ply ${move.plies}`;
  renderClocks();
  requestEval();
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
  if (e.metaKey || e.ctrlKey || e.altKey) return;
  if (e.key === "n" || e.key === "N") requestNext();
  if (e.key === "e" || e.key === "E") toggleEval();
});

loadConfig().then(applyEvalPreference);
connect();
