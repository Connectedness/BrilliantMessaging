import { defineConfig } from 'vite';
import { fileURLToPath, URL } from 'node:url';

const r = (p) => fileURLToPath(new URL(p, import.meta.url));

// Multi-page setup — one HTML entry per logo concept.
export default defineConfig({
  build: {
    rollupOptions: {
      input: {
        main: r('./index.html'),
        concept2: r('./concept2.html'),
        concept3: r('./concept3.html'),
        downloads: r('./downloads.html'),
      },
    },
  },
});
