import { Pool, PoolConfig, types } from 'pg';

// node-postgres returns BIGINT (int8) as a string to avoid precision loss above
// 2^53. Every 64-bit column in this schema — job/queue ids, lease_seq,
// lease_duration in nanoseconds — is far below that, and the domain models type
// them as `number`. Without this, `lease_seq + 1` is string concatenation.
//
// This registers on *this* package's copy of `pg`. A caller who hands us a
// client from a different copy is not covered, which is why deserializeJob
// coerces defensively as well.
types.setTypeParser(types.builtins.INT8, (value) => parseInt(value, 10));

export interface DatabaseConfig {
  /**
   * `postgres://user:pass@host:5432/db`. When set it takes precedence over the
   * discrete host/port/user/password/database fields.
   */
  connectionString?: string;
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  database?: string;
  ssl?: PoolConfig['ssl'];
  /** Shows up in `pg_stat_activity`; makes this library's sessions identifiable. */
  applicationName?: string;
  max?: number;
  idleTimeoutMillis?: number;
  connectionTimeoutMillis?: number;
}

export function createPool(config: DatabaseConfig): Pool {
  const poolConfig: PoolConfig = {
    max: config.max || 20,
    idleTimeoutMillis: config.idleTimeoutMillis || 30000,
    connectionTimeoutMillis: config.connectionTimeoutMillis || 2000,
  };

  if (config.connectionString) {
    poolConfig.connectionString = config.connectionString;
  } else {
    poolConfig.host = config.host;
    poolConfig.port = config.port;
    poolConfig.user = config.user;
    poolConfig.password = config.password;
    poolConfig.database = config.database;
  }

  if (config.ssl !== undefined) {
    poolConfig.ssl = config.ssl;
  }
  if (config.applicationName !== undefined) {
    poolConfig.application_name = config.applicationName;
  }

  return new Pool(poolConfig);
}
