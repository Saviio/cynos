import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

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
