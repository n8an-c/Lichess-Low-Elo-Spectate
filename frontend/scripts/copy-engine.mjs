// Copies the single-threaded lite Stockfish build from the npm package into
// public/engine/ so Vite serves it unchanged at /engine/... in dev and prod.
// The worker .js locates its .wasm next to itself, so both must be copied.
import { copyFileSync, existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const src = join(here, "..", "node_modules", "stockfish", "bin");
const dest = join(here, "..", "public", "engine");
const files = ["stockfish-19-lite-single.js", "stockfish-19-lite-single.wasm"];

mkdirSync(dest, { recursive: true });
for (const f of files) {
  const from = join(src, f);
  if (!existsSync(from)) {
    console.error(`copy-engine: ${from} not found; run npm install first`);
    process.exit(1);
  }
  copyFileSync(from, join(dest, f));
}
console.log(`copy-engine: copied ${files.length} files to public/engine/`);
