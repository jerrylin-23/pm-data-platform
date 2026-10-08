import { defineConfig } from 'vite';
export default defineConfig({ base: '/assets/', build: { outDir: '../src/pmplatform/static', emptyOutDir: true, assetsDir: '' } });
