// React 19 + Vite + Tailwind v4, a static SPA. `npm run build` emits a
// flat dist/ that the Controller go:embed's — no Node in production, no
// meta-framework (see architecture.md, "Console (frontend)").
import path from 'node:path'
import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import { defineConfig, type Plugin } from 'vite'

function consoleVersion(): Plugin {
  return {
    name: 'groundplane-console-version',
    generateBundle() {
      const release = process.env.GP_CONSOLE_RELEASE
      if (!release) return
      if (!/^console\/v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(release)) {
        this.error('GP_CONSOLE_RELEASE must be a published Console tag')
      }
      this.emitFile({
        type: 'asset',
        fileName: 'console-version.json',
        source: JSON.stringify({ release }),
      })
    },
  }
}

export default defineConfig({
  plugins: [react(), tailwindcss(), consoleVersion()],
  resolve: {
    alias: { '@': path.resolve(import.meta.dirname, './src') },
  },
  server: {
    proxy: {
      '/api': {
        target: process.env.GP_API_URL || 'http://127.0.0.1:8080',
        changeOrigin: false,
        ws: true,
      },
    },
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
