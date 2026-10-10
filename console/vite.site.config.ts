import path from "node:path";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

export default defineConfig(({ isSsrBuild }) => ({
  root: path.resolve(import.meta.dirname, "../site"),
  publicDir: false,
  plugins: [react(), tailwindcss()],
  resolve: { alias: { "@": path.resolve(import.meta.dirname, "src") } },
  ssr: { noExternal: true },
  build: {
    emptyOutDir: false,
    sourcemap: false,
    rollupOptions: isSsrBuild
      ? { output: { entryFileNames: "render.mjs" } }
      : undefined,
  },
}));
