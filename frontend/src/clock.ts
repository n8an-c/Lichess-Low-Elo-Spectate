// Pure clock helpers (no DOM) so the logic is easy to reason about and test.

import type { ViewerState } from "./types";

export type Color = "white" | "black";

export function sideToMove(fen: string | null): Color {
  return fen && fen.split(" ")[1] === "b" ? "black" : "white";
}

/** Lichess does not count time on each side's first move, so clocks tick from ply 2. */
export function clockRunning(state: Pick<ViewerState, "status" | "plies">): boolean {
  return state.status === "live" && state.plies >= 2;
}

/**
 * Seconds left for one side. `atMs` is when `seconds` was received (server
 * epoch ms); the running side loses the time elapsed since then.
 */
export function remainingSeconds(
  seconds: number | null,
  atMs: number | null,
  nowMs: number,
  running: boolean,
): number | null {
  if (seconds === null) return null;
  if (!running || atMs === null) return Math.max(0, seconds);
  return Math.max(0, seconds - (nowMs - atMs) / 1000);
}

/** m:ss, or s.t (tenths) under 10 seconds. */
export function formatClock(seconds: number | null): string {
  if (seconds === null) return "–:––";
  const s = Math.max(0, seconds);
  if (s < 10) return s.toFixed(1);
  const m = Math.floor(s / 60);
  const rest = Math.floor(s - m * 60);
  return `${m}:${rest.toString().padStart(2, "0")}`;
}

const STATUS_TEXT: Record<string, (winner: Color | null) => string> = {
  mate: () => "Checkmate",
  resign: (w) => `${cap(other(w))} resigned`,
  outoftime: (w) => `${cap(other(w))} ran out of time`,
  timeout: (w) => `${cap(other(w))} left the game`,
  stalemate: () => "Stalemate",
  draw: () => "Draw",
  aborted: () => "Game aborted",
  noStart: () => "Game aborted (no start)",
  cheat: () => "Game ended (cheat detected)",
  unknownFinish: () => "Game over",
  disconnected: () => "Stream disconnected",
};

function other(w: Color | null): string {
  return w === "white" ? "black" : w === "black" ? "white" : "a player";
}

function cap(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/** "Checkmate · Black wins", "Draw (stalemate)", "Game aborted", ... */
export function describeResult(result: { status: string; winner: Color | null } | null): string {
  if (!result) return "Game over";
  const text = (STATUS_TEXT[result.status] ?? (() => `Game over (${result.status})`))(result.winner);
  if (result.winner) return `${text} · ${cap(result.winner)} wins`;
  if (result.status === "stalemate") return "Draw (stalemate)";
  return text;
}
