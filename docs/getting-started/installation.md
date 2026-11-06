# Installation

## Requirements

- Node.js 18 or later
- PostgreSQL 12 or later (the test suite runs against PostgreSQL 16)
- TypeScript 5 or later, if you use TypeScript. Type declarations ship with
  the package.

## Install the package

```bash
npm install distributed-task-queue
```

> [!NOTE]
> The package has not been published to npm yet. Until it is, build a local
> checkout (`npm install && npm run build`) and install it from that folder:
> `npm install /path/to/distributed-task-queue`.

The package depends on [`pg`](https://node-postgres.com) (node-postgres) 8.x,
which is installed with it. You hand the library a `pg` pool, and for
transactional publishing, a `pg` client.

## Create the database schema

The library does not create its tables at startup. Apply the bundled
migrations once per database, before any producer or worker connects. After
upgrading the package, apply any new migrations before deploying code that
needs them.

The schema creates five tables with fixed, unqualified names: `queues`, `jobs`,
`queue_shards`, `group_queue_limits` and, if you use the bundled runner,
`schema_migrations`. They are created in the first schema on the connection's
`search_path`, usually `public`. **Check that none of these names are already
taken.** The migration uses `CREATE TABLE IF NOT EXISTS`, so an existing table
called `jobs` is silently kept, and the library then fails in confusing ways.

### Option 1: the bundled runner

```bash
DATABASE_HOST=localhost \
DATABASE_PORT=5432 \
DATABASE_USER=user \
DATABASE_PASS=password \
DATABASE_NAME=queue \
MIGRATIONS_PATH=node_modules/distributed-task-queue/migrations \
node node_modules/distributed-task-queue/dist/migration/runner.js up
```

```text
Applying 1 migration(s)...
Applied migration 1_initial_schema (up)
Migrations applied successfully
```

Running it again is harmless: it prints `No migrations to apply`.

The runner reads these variables from the environment, or from a `.env` file in
the current directory:

| Variable          | Default      | Meaning                                       |
| ----------------- | ------------ | --------------------------------------------- |
| `DATABASE_HOST`   | `localhost`  | Server host                                   |
| `DATABASE_PORT`   | `5432`       | Server port                                   |
| `DATABASE_USER`   | `user`       | Role to connect as                            |
| `DATABASE_PASS`   | `password`   | Password for that role                        |
| `DATABASE_NAME`   | `queue`      | Database to migrate                           |
| `MIGRATIONS_PATH` | `migrations` | Folder with the `.sql` files, relative to the current directory |

The runner takes no connection string. If you need one, or TLS options, use
option 2.

Each migration runs in its own transaction and is recorded in
`schema_migrations`. To roll back, run the same command with `down` instead of
`up`. It rolls back one migration by default; add a number to roll back more,
for example `down 2`.

> [!WARNING]
> Rolling back `000001_initial_schema` drops all of the queue tables, including
> every queued job.

### Option 2: your own migration tool

The migrations are plain PostgreSQL files in
`node_modules/distributed-task-queue/migrations/`, one pair per version:

```text
000001_initial_schema.up.sql
000001_initial_schema.down.sql
```

Copy them into the tool that already manages your schema (Flyway, Knex,
Prisma, golang-migrate, …) and apply the `.up.sql` files in version order. They
contain only DDL and can run inside a transaction.

## Check the setup

```bash
psql "postgres://user:password@localhost:5432/queue" -c '\dt'
```

You should see `group_queue_limits`, `jobs`, `queue_shards` and `queues`. Then
continue with the [Quick start](quick-start.md).
