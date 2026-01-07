import { Pool, PoolConfig } from 'pg';

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

