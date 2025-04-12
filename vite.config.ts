// vite.config.ts
import { defineConfig } from 'vite';
import { configDefaults } from 'vitest/config';

export default defineConfig({
  test: {
    // Set the test timeout to 60 seconds
    testTimeout: 60000,
    // You can also configure other test options here
    ...configDefaults,
  },
});