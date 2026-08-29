# PR stack — 16 stacked pull requests

Splits `6c29d92` → the full v1 working tree. One file per PR in this folder:
open the compare link, paste the title, paste the description, create.

**Merge in order, and use "Rebase and merge".** A merge commit on PR01 leaves
PR02's base pointing at the pre-merge commit and inflates its diff.

| # | File | Scope | Diff |
|---|---|---|---|
| 01 | [01-tooling.md](01-tooling.md) | deps, lockfiles, compose | 5 files |
| 02 | [02-schema.md](02-schema.md) | migrations 000001 + 000002 | 78+ / 39− |
| 03 | [03-storage.md](03-storage.md) | **single round-trip sharded pull, lease fencing, dual-path settle** | 666+ / 243− |
| 04 | [04-worker-service.md](04-worker-service.md) | concurrent slot loop | 94+ |
| 05 | [05-benchmarks.md](05-benchmarks.md) | perf harnesses | 14 files |
| 06 | [06-test-harness.md](06-test-harness.md) | testcontainers harness | 909+ |
| 07 | [07-storage-tests.md](07-storage-tests.md) | lifecycle / concurrency / crash | 1048+ |
| 08 | [08-observability.md](08-observability.md) | logger, Executor, pool config | 131+ / 36− |
| 09 | [09-publish.md](09-publish.md) | **transactional + batched + dedup publish** | 190+ / 45− |
| 10 | [10-queue-resolution.md](10-queue-resolution.md) | `ensureQueue`, drift warning | 98+ / 11− |
| 11 | [11-worker-hardening.md](11-worker-hardening.md) | poison discard, bounded shutdown | 336+ / 41− |
| 12 | [12-listen-notify.md](12-listen-notify.md) | `PgNotifier` + `pg_notify` | 383+ / 9− |
| 13 | [13-client-facade.md](13-client-facade.md) | `src/client/`, narrowed exports | 686+ / 15− |
| 14 | [14-examples.md](14-examples.md) | examples on the public API | 223+ / 88− |
| 15 | [15-facade-tests.md](15-facade-tests.md) | 5 API suites | 4517+ |
| 16 | [16-docs.md](16-docs.md) | README, checklist, test report | 2226+ |

## Two blockers, verified in the code

- **PR03** — `recoverJobs` orders `created_at DESC` under `LIMIT`: the reaper
  reclaims the *newest* expired jobs, so the longest-stranded ones starve.
- **PR09** — `idempotency_key` is globally unique but the read-back has no
  `queue_id` filter: a cross-queue key collision drops the publish and returns
  another queue's job with `deduplicated: true`.

## State of the branches

- `origin/main` = `6c29d92`. Tag `backup/pre-split-936d613` holds the old
  `main`; revert the whole thing with
  `git push -f origin backup/pre-split-936d613:main`.
- PRs 01–02 reproduce the old `936d613` byte-for-byte. From PR03 on, the
  storage layer is the single-round-trip CTE rewrite: each stage's tree equals
  the original stage's tree with only `job.repository.ts` swapped (plus
  `tests/producer.test.ts` from PR15, whose recorder keeps whole statements).
  The pre-fold stack survives at tag `backup/pre-fold-8e79991`.
- PR16's tree is identical to the verified single-round-trip tree (the one the
  full 201-test suite ran against), apart from doc updates in README /
  CLAUDE.md / V1_CHECKLIST that describe the single-statement design.
- Every one of the 16 stages typechecks on its own; the suites at stages 07, 09
  and 12 run 55 passed / 1 known-red.

## Known red test

`npm test` → **200 passed, 1 failed** (201 total). The failure is the
group-fairness marker in `concurrency.property.test.ts`, deferred to v2 in
`V1_CHECKLIST.md`. It arrives in PR07 and stays red through the stack. It is
pre-existing, not introduced by the split — if you want CI green it needs an
explicit skip-with-reason.

## Not done, on purpose

- `compose.yaml` mounts `./postgres_data:/var/lib/postgresql/datavolumes` —
  the target path looks wrong. Left as-is (PR01) because it is your committed
  state, not something the split introduced.
- The three root `*_SUMMARY` / `*_IMPLEMENTATION` markdown files are kept so
  PR16 reproduces the tree exactly.
