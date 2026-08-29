# PR16 — docs: README, architecture notes, test report and v1 checklist

| | |
|---|---|
| **base** | `split/15-facade-tests` |
| **head** | `split/16-docs` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/15-facade-tests...split/16-docs?expand=1 |

## Title

```
docs: README, architecture notes, test report and v1 checklist
```

## Description

---

**Check:**
- `V1_CHECKLIST.md` is the one to read — an audit of what's missing before this ships, with line references. Both blockers flagged in PR03 and PR09 are tracked there.
- Packaging, none fixed in this stack: no `files` field (`npm pack` ships 169 files / 245 kB incl. tests and a PNG); no `prepublishOnly` and `dist/` is gitignored, so `main` wouldn't exist in the published package; version says 1.0.0; `LICENSE` still says "Iman Tumorang"; `dependabot.yml` is set to gomod; the only CI workflow is `gotest.yml` and is always red.
- `CLAUDE.md` documents a `job_status` archive table that no migration creates.
- `CLAUDE.md` says shards get `floor(concurrency / 32)`; the code spreads the remainder. Stale.
- `TEST_REPORT.md` counts (56) predate `V1_CHECKLIST.md` (195). Actual run today: 201.
- The three root `*_SUMMARY`/`*_IMPLEMENTATION` files are generated scratch notes — happy to drop them.
