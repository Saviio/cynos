import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    browser: {
      enabled: true,
      name: 'chromium',
      provider: 'playwright',
    },
    include: ['tests/**/*.test.ts'],
    testTimeout: 30000,
  },
});
