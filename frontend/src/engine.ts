// Stockfish WASM wrapper: one Web Worker speaking UCI, analysing only the
// latest requested position at a fixed, modest depth.

import { parseInfo, toWhitePerspective, type EngineEval } from "./evalmath";

export const ENGINE_URL = "/engine/stockfish-19-lite-single.js";

export interface EvalUpdate extends EngineEval {
  fen: string;
}

export interface EngineHandlers {
  onReady?: () => void;
  onEval?: (update: EvalUpdate) => void;
  onIdle?: (fen: string) => void;
  onError?: (message: string) => void;
}

export class Engine {
  private worker: Worker | null = null;
  private ready = false;
  private currentFen: string | null = null; // position the engine is searching
  private pendingFen: string | null = null; // requested while a search was being stopped
  private searching = false;
  private stopping = false;

  constructor(
    private depth: number,
    private handlers: EngineHandlers = {},
  ) {}

  get isReady(): boolean {
    return this.ready;
  }

  setDepth(depth: number) {
    this.depth = depth;
  }

  start() {
    if (this.worker) return;
    try {
      this.worker = new Worker(ENGINE_URL);
    } catch (e) {
      this.handlers.onError?.(`cannot start engine worker: ${String(e)}`);
      return;
    }
    this.worker.onerror = (e) => {
      this.handlers.onError?.(`engine worker error: ${e.message || "failed to load " + ENGINE_URL}`);
      this.stop();
    };
    this.worker.onmessage = (ev: MessageEvent) => this.onLine(String(ev.data));
    this.send("uci");
  }

  stop() {
    this.worker?.terminate();
    this.worker = null;
    this.ready = false;
    this.searching = false;
    this.stopping = false;
    this.currentFen = null;
    this.pendingFen = null;
  }

  /** Analyse `fen`; supersedes any running or pending search. */
  evaluate(fen: string) {
    if (!this.worker) return;
    if (!this.ready) {
      this.pendingFen = fen;
      return;
    }
    if (this.searching) {
      this.pendingFen = fen;
      if (!this.stopping) {
        this.stopping = true;
        this.send("stop"); // the `bestmove` reply starts the pending search
      }
      return;
    }
    this.go(fen);
  }

  private go(fen: string) {
    this.currentFen = fen;
    this.searching = true;
    this.stopping = false;
    this.send(`position fen ${fen}`);
    this.send(`go depth ${this.depth}`);
  }

  private send(cmd: string) {
    this.worker?.postMessage(cmd);
  }

  private onLine(line: string) {
    if (line === "uciok") {
      this.send("setoption name Hash value 16");
      this.send("isready");
      return;
    }
    if (line === "readyok") {
      if (!this.ready) {
        this.ready = true;
        this.handlers.onReady?.();
        if (this.pendingFen) {
          const fen = this.pendingFen;
          this.pendingFen = null;
          this.go(fen);
        }
      }
      return;
    }
    if (line.startsWith("bestmove")) {
      const finished = this.currentFen;
      this.searching = false;
      this.stopping = false;
      this.currentFen = null;
      if (this.pendingFen) {
        const fen = this.pendingFen;
        this.pendingFen = null;
        this.go(fen);
      } else if (finished) {
        this.handlers.onIdle?.(finished);
      }
      return;
    }
    if (line.startsWith("info ") && this.currentFen && !this.stopping) {
      const parsed = parseInfo(line);
      if (!parsed) return;
      const blackToMove = this.currentFen.split(" ")[1] === "b";
      this.handlers.onEval?.({ fen: this.currentFen, ...toWhitePerspective(parsed, blackToMove) });
    }
  }
}
