// Pure helpers for engine output: UCI info parsing, perspective, bar height, labels.

export interface Score {
  /** Centipawns from white's perspective (undefined when a mate is reported). */
  cp?: number;
  /** Moves to mate from white's perspective: positive = white mates, negative = black mates, 0 = side to move is mated. */
  mate?: number;
}

export interface EngineEval extends Score {
  depth: number;
}

/** Parse a UCI `info ... depth D ... score cp X|mate N ...` line (side-to-move perspective). */
export function parseInfo(line: string): EngineEval | null {
  if (!line.startsWith("info ")) return null;
  const parts = line.split(/\s+/);
  let depth: number | null = null;
  let cp: number | undefined;
  let mate: number | undefined;
  for (let i = 1; i < parts.length; i++) {
    if (parts[i] === "depth") depth = Number(parts[i + 1]);
    else if (parts[i] === "score") {
      if (parts[i + 1] === "cp") cp = Number(parts[i + 2]);
      else if (parts[i + 1] === "mate") mate = Number(parts[i + 2]);
    } else if (parts[i] === "lowerbound" || parts[i] === "upperbound") return null; // partial result, skip
  }
  if (depth === null || (cp === undefined && mate === undefined)) return null;
  return { depth, cp, mate };
}

/** Flip a side-to-move score to white's perspective. */
export function toWhitePerspective(e: EngineEval, blackToMove: boolean): EngineEval {
  if (!blackToMove) return e;
  return {
    depth: e.depth,
    cp: e.cp === undefined ? undefined : -e.cp,
    mate: e.mate === undefined ? undefined : e.mate === 0 ? 0 : -e.mate,
  };
}

/**
 * White's winning chance in 0..1 for the bar height. Uses Lichess's centipawn
 * curve; a mate score (from white's perspective) pins the bar to an end.
 */
export function whiteShare(score: Score, blackToMove = false): number {
  if (score.mate !== undefined) {
    if (score.mate === 0) return blackToMove ? 1 : 0; // side to move is mated
    return score.mate > 0 ? 1 : 0;
  }
  const cp = Math.max(-1500, Math.min(1500, score.cp ?? 0));
  return 1 / (1 + Math.exp(-0.00368208 * cp));
}

/** "+0.8", "-2.3", "0.0", "#3", "#-2", "#" (mated). */
export function formatEval(score: Score): string {
  if (score.mate !== undefined) {
    if (score.mate === 0) return "#";
    return score.mate > 0 ? `#${score.mate}` : `#-${-score.mate}`;
  }
  const pawns = (score.cp ?? 0) / 100;
  if (Math.abs(pawns) < 0.05) return "0.0";
  return `${pawns > 0 ? "+" : ""}${pawns.toFixed(1)}`;
}
