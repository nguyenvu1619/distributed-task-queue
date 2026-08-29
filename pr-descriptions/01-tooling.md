# PR01 — build: test/bench toolchain and pinned dependency tree

| | |
|---|---|
| **base** | `main` |
| **head** | `split/01-tooling` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/main...split/01-tooling?expand=1 |

## Title

```
build: test/bench toolchain and pinned dependency tree
```

## Description

---

Dev dependencies (vitest, testcontainers) plus a lockfile. No source changes.

**Check:**
- `compose.yaml` mounts `./postgres_data:/var/lib/postgresql/datavolumes` — target path looks wrong, and the named volume became a repo bind mount. Intended?
- Both `package-lock.json` and `yarn.lock` are committed. Pick one.
- `max_connections=200`; pool default is 20/process, so ~10 processes.
