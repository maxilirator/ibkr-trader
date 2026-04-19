import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [sveltekit()],
  server: {
    allowedHosts: ['quant.geisler.se']
  },
  preview: {
    allowedHosts: ['quant.geisler.se']
  }
});
