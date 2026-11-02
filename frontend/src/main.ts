import { Chessground } from "chessground";
import type { Api } from "chessground/api";
import type { Key } from "chessground/types";
import "./style.css";
import type { MoveEvent, ViewerState } from "./types";

const boardEl = document.getElementById("board")!;
const statusEl = document.getElementById("status")!;
const linkEl = document.getElementById("link")!;
const nextBtn = document.getElementById("next") as HTMLButtonElement;

const START_FEN = "rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1";

const cg: Api = Chessground(boardEl, {
  fen: START_FEN,
  orientation: "white",
  viewOnly: true,
  coordinates: true,
  animation: { duration: 150 },
  highlight: { lastMove: true, check: true },
});

let current: ViewerState | null = null;

function sideToMove(fen: string): "white" | "black" {
  return fen.split(" ")[1] === "b" ? "black" : "white";
}

function uciToKeys(uci: string | null): Key[] | undefined {
  if (!uci || uci.length < 4) return undefined;
  return [uci.slice(0, 2) as Key, uci.slice(2, 4) as Key];
}

function applyPosition(fen: string | null, lastMove: string | null) {
  const f = fen ?? START_FEN;
  cg.set({ fen: f, lastMove: uciToKeys(lastMove), turnColor: sideToMove(f) });
}

function setPlayer(id: "white" | "black", state: ViewerState) {
  const el = document.getElementById(id)!;
  const p = state.game?.[id];
  el.querySelector(".name")!.textContent = p ? (p.title ? `${p.title} ${p.name}` : p.name) : "—";
  el.querySelector(".rating")!.textContent = p ? String(p.rating) : "";
}

function describeResult(state: ViewerState): string {
  const r = state.result;
  if (!r) return "Game over";
  const winner = r.winner ? `${r.winner} wins` : "draw";
  return `Game over: ${winner} (${r.status})`;
}

function render(state: ViewerState) {
  current = state;
  setPlayer("white", state);
  setPlayer("black", state);
  applyPosition(state.fen, state.lastMove);
  if (state.status === "waiting") {
    statusEl.textContent = "Waiting for a qualifying game…";
    linkEl.textContent = "";
  } else if (state.status === "live") {
    statusEl.textContent = `Live · ply ${state.plies}`;
  } else {
    statusEl.textContent = describeResult(state);
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
}

function onMove(move: MoveEvent) {
  if (!current) return;
  current = { ...current, fen: move.fen, lastMove: move.lastMove, plies: move.plies, wc: move.wc, bc: move.bc };
  applyPosition(move.fen, move.lastMove);
  statusEl.textContent = `Live · ply ${move.plies}`;
}

function connect() {
  const es = new EventSource("/api/events");
  es.addEventListener("state", (e) => render(JSON.parse((e as MessageEvent).data)));
  es.addEventListener("game", (e) => render(JSON.parse((e as MessageEvent).data)));
  es.addEventListener("waiting", (e) => render(JSON.parse((e as MessageEvent).data)));
  es.addEventListener("end", (e) => render(JSON.parse((e as MessageEvent).data)));
  es.addEventListener("move", (e) => onMove(JSON.parse((e as MessageEvent).data)));
  es.onerror = () => {
    statusEl.textContent = "Connection lost, reconnecting…";
    // EventSource reconnects on its own; the first event after reconnect is a full `state`.
  };
}

nextBtn.addEventListener("click", async () => {
  nextBtn.disabled = true;
  try {
    await fetch("/api/next", { method: "POST" });
  } finally {
    nextBtn.disabled = false;
  }
});

connect();
