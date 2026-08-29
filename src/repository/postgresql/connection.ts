import { Pool, PoolConfig, types } from 'pg';

// node-postgres returns BIGINT (int8) as a string to avoid precision loss above
// 2^53. Every 64-bit column in this schema — job/queue ids, lease_seq,
// lease_duration in nanoseconds — is far below that, and the domain models type
// them as `number`. Without this, `lease_seq + 1` is string concatenation.
types.setTypeParser(types.builtins.INT8, (value) => parseInt(value, 10));

export interface DatabaseConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  max?: number;
  idleTimeoutMillis?: number;
  connectionTimeoutMillis?: number;
}

export function createPool(config: DatabaseConfig): Pool {
  const poolConfig: PoolConfig = {
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    max: config.max || 20,
    idleTimeoutMillis: config.idleTimeoutMillis || 30000,
    connectionTimeoutMillis: config.connectionTimeoutMillis || 2000,
  };

  return new Pool(poolConfig);
}

