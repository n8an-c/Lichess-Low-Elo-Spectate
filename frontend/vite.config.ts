import { defineConfig } from "vite";

// `npm run dev` serves the frontend on :5173 and proxies the API (including
// the SSE stream) to the Python backend on :8000.
export default defineConfig({
  server: {
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
  },
});
