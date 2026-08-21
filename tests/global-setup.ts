import * as path from 'path';
import { PostgreSqlContainer, StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { TestProject } from 'vitest/node';

import { createPool, DatabaseConfig } from '../src/repository/postgresql/connection';
import { migrateUp } from '../src/migration/runner';

/**
 * Connection details handed to every test file via vitest's `inject()`.
 * Also mirrored onto process.env so that child processes spawned by the
 * crash/fencing suite can pick them up without extra plumbing.
 */
export interface PgHandle {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
}

declare module 'vitest' {
  export interface ProvidedContext {
    pg: PgHandle;
  }
}

const IMAGE = process.env.TEST_PG_IMAGE || 'postgres:16';
const MIGRATIONS_PATH = path.resolve(__dirname, '..', 'migrations');

/**
 * Set TEST_PG_EXTERNAL=1 to run against an already-running Postgres
 * (e.g. `docker compose up -d`) instead of spinning up a container.
 * Useful for fast local iteration and for CI runners without docker-in-docker.
 */
function externalHandle(): PgHandle | null {
  if (process.env.TEST_PG_EXTERNAL !== '1') return null;
  return {
    host: process.env.DATABASE_HOST || 'localhost',
    port: parseInt(process.env.DATABASE_PORT || '5432', 10),
    user: process.env.DATABASE_USER || 'user',
    password: process.env.DATABASE_PASS || 'password',
    database: process.env.DATABASE_NAME || 'queue',
  };
}

/**
 * Fail with something actionable instead of a raw `ECONNREFUSED` / `database
 * "x" does not exist` from deep inside the migration runner.
 */
async function preflight(handle: PgHandle): Promise<void> {
  const pool = createPool({ ...handle, max: 1, connectionTimeoutMillis: 5_000 });
  try {
    await pool.query('SELECT 1');
  } catch (error) {
    throw new Error(
      `[test-harness] cannot reach Postgres at ${handle.host}:${handle.port}/${handle.database} as "${handle.user}".\n` +
        `  ${(error as Error).message}\n` +
        `  TEST_PG_EXTERNAL=1 reads DATABASE_HOST/PORT/USER/PASS/NAME from the environment,\n` +
        `  falling back to .env — check those match the database you meant.\n` +
        `  Start the project database with:  docker compose up -d\n` +
        `  Or unset TEST_PG_EXTERNAL to let testcontainers start a throwaway one.`
    );
  } finally {
    await pool.end();
  }
}

async function waitForMigrations(handle: PgHandle): Promise<void> {
  const config: DatabaseConfig = { ...handle, max: 4 };
  const pool = createPool(config);
  try {
    await migrateUp(pool, MIGRATIONS_PATH);
  } finally {
    await pool.end();
  }
}

function publish(handle: PgHandle, project?: TestProject) {
  process.env.DATABASE_HOST = handle.host;
  process.env.DATABASE_PORT = String(handle.port);
  process.env.DATABASE_USER = handle.user;
  process.env.DATABASE_PASS = handle.password;
  process.env.DATABASE_NAME = handle.database;
  project?.provide('pg', handle);
}

export default async function setup(project: TestProject) {
  const external = externalHandle();

  if (external) {
    // Name the database explicitly: every suite TRUNCATEs it, so it must be
    // obvious which one is about to be wiped.
    console.log(
      `[test-harness] using external Postgres — ${external.user}@${external.host}:${external.port}/${external.database}`
    );
    console.log('[test-harness] every table in that database will be TRUNCATEd between tests');
    await preflight(external);
    await waitForMigrations(external);
    publish(external, project);
    return async () => {
      /* nothing to tear down — we do not own this database */
    };
  }

  console.log(`[test-harness] starting ${IMAGE} via testcontainers...`);
  const started: StartedPostgreSqlContainer = await new PostgreSqlContainer(IMAGE)
    .withDatabase('queue')
    .withUsername('user')
    .withPassword('password')
    // Mirrors compose.yaml: the concurrency suites open a lot of sessions.
    .withCommand(['postgres', '-c', 'max_connections=200'])
    .start();

  const handle: PgHandle = {
    host: started.getHost(),
    port: started.getPort(),
    user: started.getUsername(),
    password: started.getPassword(),
    database: started.getDatabase(),
  };

  console.log(`[test-harness] Postgres ready at ${handle.host}:${handle.port}`);
  await waitForMigrations(handle);
  publish(handle, project);

  return async () => {
    console.log('[test-harness] stopping Postgres container');
    await started.stop();
  };
}
