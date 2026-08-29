/**
 * The minimum surface a database handle has to expose to run a statement.
 *
 * Deliberately structural rather than `Pool | PoolClient`: a pg `Pool` does not
 * extend `ClientBase`, so TypeScript resolves overloads on that union poorly,
 * and a structural type also admits an adapter around a Knex / Kysely / Drizzle
 * transaction handle. Anything that can run `query(text, values)` and hand back
 * rows can carry a publish.
 */
export interface QueryResultLike {
  rows: any[];
  rowCount: number | null;
}

export interface Executor {
  query(text: string, values?: any[]): Promise<QueryResultLike>;
}
