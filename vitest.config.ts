import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['tests/**/*.test.ts'],
    globalSetup: ['./tests/global-setup.ts'],

    // Every suite talks to the same Postgres instance and TRUNCATEs between
    // tests, so files must not overlap. Concurrency assertions also need the
    // database to themselves to be meaningful.
    fileParallelism: false,
    pool: 'forks',
    poolOptions: { forks: { singleFork: true } },

    // Container pull + migrations on a cold cache, plus tests that deliberately
    // wait for leases to expire.
    testTimeout: 120_000,
    hookTimeout: 120_000,
    teardownTimeout: 60_000,

    reporters: ['verbose'],
  },
});
