import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'path'
import { copyFileSync, existsSync } from 'fs'

// Copy wasm file to correct location in node_modules
// This is needed because @cynos/core's wasm.js looks for cynos_database_bg.wasm
const wasmSrc = resolve(__dirname, 'node_modules/@cynos/core/dist/database.wasm')
const wasmDest = resolve(__dirname, 'node_modules/@cynos/core/dist/cynos_database_bg.wasm')

if (existsSync(wasmSrc) && !existsSync(wasmDest)) {
  copyFileSync(wasmSrc, wasmDest)
}

export default defineConfig({
  plugins: [react()],
  optimizeDeps: {
    exclude: ['@cynos/core']
  },
  server: {
    fs: {
      allow: ['..']
    }
  }
})
