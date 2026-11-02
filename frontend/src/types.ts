// Payloads sent by the backend (see backend/viewer.py: ViewerState.to_dict).

export interface PlayerInfo {
  id: string;
  name: string;
  rating: number;
  title: string | null;
}

export interface GameInfo {
  id: string;
  url: string;
  white: PlayerInfo;
  black: PlayerInfo;
  createdAt: number;
  pliesAtJoin: number;
}

export type ViewerStatus = "waiting" | "live" | "finished";

export interface GameResult {
  status: string; // Lichess status name: mate, resign, draw, outoftime, ...
  winner: "white" | "black" | null;
}

export interface ViewerState {
  status: ViewerStatus;
  game: GameInfo | null;
  fen: string | null;
  lastMove: string | null; // UCI, e.g. "e2e4"
  plies: number;
  wc: number | null; // white clock, seconds, at the last move
  bc: number | null;
  result: GameResult | null;
  updatedAt: number;
}

export interface MoveEvent {
  fen: string;
  lastMove: string | null;
  plies: number;
  wc: number | null;
  bc: number | null;
}
