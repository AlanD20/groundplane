// React 19 + Vite + Tailwind v4, a static SPA. `npm run build` emits a
// flat dist/ that the Controller go:embed's — no Node in production, no
// meta-framework (see architecture.md, "Console (frontend)").
import path from 'node:path'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { '@': path.resolve(import.meta.dirname, './src') },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      output: {
        hashCharacters: 'hex',
        entryFileNames: 'assets/[name]-[hash:8].js',
        chunkFileNames: 'assets/[name]-[hash:8].js',
        assetFileNames: 'assets/[name]-[hash:8][extname]',
      },
    },
  },
})
