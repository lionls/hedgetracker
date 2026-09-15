import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// In development Vite serves the app and proxies /api to the Go server;
// in production the Go server serves the built bundle from web/dist itself.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { '/api': 'http://127.0.0.1:8080' },
  },
  build: { outDir: 'dist', emptyOutDir: true },
});
