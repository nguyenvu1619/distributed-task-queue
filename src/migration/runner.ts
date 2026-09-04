import { Pool } from 'pg';
import * as fs from 'fs';
import * as path from 'path';
import { createPool, DatabaseConfig } from '../repository/postgresql/connection';
import * as dotenv from 'dotenv';

dotenv.config();

interface MigrationFile {
  version: number;
  name: string;
  upPath: string;
  downPath: string;
}

export async function ensureMigrationsTable(pool: Pool): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version BIGINT NOT NULL PRIMARY KEY,
      dirty BOOLEAN NOT NULL DEFAULT FALSE
    )
  `);
}

export function parseMigrationFiles(migrationsPath: string): MigrationFile[] {
  const files = fs.readdirSync(migrationsPath);
  const migrations: MigrationFile[] = [];

  const migrationMap = new Map<number, { up?: string; down?: string; name?: string }>();

  for (const file of files) {
    const match = file.match(/^(\d+)_(.+)\.(up|down)\.sql$/);
    if (match) {
      const version = parseInt(match[1], 10);
      const name = match[2];
      const direction = match[3];

      if (!migrationMap.has(version)) {
        migrationMap.set(version, { name });
      }

      const migration = migrationMap.get(version)!;
      if (direction === 'up') {
        migration.up = path.join(migrationsPath, file);
      } else {
        migration.down = path.join(migrationsPath, file);
      }
    }
  }

  for (const [version, data] of migrationMap.entries()) {
    if (data.up && data.down && data.name) {
      migrations.push({
        version,
        name: data.name,
        upPath: data.up,
        downPath: data.down,
      });
    }
  }

  return migrations.sort((a, b) => a.version - b.version);
}

async function getCurrentVersion(pool: Pool): Promise<number> {
  const result = await pool.query(
    'SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1'
  );
  return result.rows.length > 0 ? parseInt(result.rows[0].version, 10) : 0;
}

async function applyMigration(pool: Pool, migration: MigrationFile, direction: 'up' | 'down'): Promise<void> {
  const filePath = direction === 'up' ? migration.upPath : migration.downPath;
  const sql = fs.readFileSync(filePath, 'utf-8');

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Mark as dirty
    await client.query(
      'INSERT INTO schema_migrations (version, dirty) VALUES ($1, true) ON CONFLICT (version) DO UPDATE SET dirty = true',
      [migration.version]
    );

    // Execute migration
    await client.query(sql);

    // Update version and mark as clean
    if (direction === 'up') {
      await client.query(
        'INSERT INTO schema_migrations (version, dirty) VALUES ($1, false) ON CONFLICT (version) DO UPDATE SET dirty = false',
        [migration.version]
      );
    } else {
      await client.query('DELETE FROM schema_migrations WHERE version = $1', [migration.version]);
    }

    await client.query('COMMIT');
    console.log(`Applied migration ${migration.version}_${migration.name} (${direction})`);
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function migrateUp(pool: Pool, migrationsPath: string): Promise<void> {
  await ensureMigrationsTable(pool);
  const migrations = parseMigrationFiles(migrationsPath);
  const currentVersion = await getCurrentVersion(pool);

  const pendingMigrations = migrations.filter((m) => m.version > currentVersion);

  if (pendingMigrations.length === 0) {
    console.log('No migrations to apply');
    return;
  }

  console.log(`Applying ${pendingMigrations.length} migration(s)...`);

  for (const migration of pendingMigrations) {
    await applyMigration(pool, migration, 'up');
  }

  console.log('Migrations applied successfully');
}

export async function migrateDown(pool: Pool, migrationsPath: string, steps: number = 1): Promise<void> {
  await ensureMigrationsTable(pool);
  const migrations = parseMigrationFiles(migrationsPath);
  const currentVersion = await getCurrentVersion(pool);

  if (currentVersion === 0) {
    console.log('No migrations to rollback');
    return;
  }

  const migrationsToRollback = migrations
    .filter((m) => m.version <= currentVersion)
    .sort((a, b) => b.version - a.version)
    .slice(0, steps);

  if (migrationsToRollback.length === 0) {
    console.log('No migrations to rollback');
    return;
  }

  console.log(`Rolling back ${migrationsToRollback.length} migration(s)...`);

  for (const migration of migrationsToRollback) {
    await applyMigration(pool, migration, 'down');
  }

  console.log('Migrations rolled back successfully');
}

async function main() {
  const direction = process.argv[2] || 'up';
  const steps = process.argv[3] ? parseInt(process.argv[3], 10) : undefined;

  const config: DatabaseConfig = {
    host: process.env.DATABASE_HOST || 'localhost',
    port: parseInt(process.env.DATABASE_PORT || '5432', 10),
    user: process.env.DATABASE_USER || 'user',
    password: process.env.DATABASE_PASS || 'password',
    database: process.env.DATABASE_NAME || 'queue',
  };

  const migrationsPath = process.env.MIGRATIONS_PATH || 'migrations';

  const pool = createPool(config);

  try {
    if (direction === 'up') {
      await migrateUp(pool, migrationsPath);
    } else if (direction === 'down') {
      await migrateDown(pool, migrationsPath, steps);
    } else {
      console.error('Invalid direction. Use "up" or "down"');
      process.exit(1);
    }
  } catch (error) {
    console.error('Migration failed:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  main();
}

