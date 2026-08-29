# PR05 — bench: performance and latency harnesses

| | |
|---|---|
| **base** | `split/04-worker-service` |
| **head** | `split/05-benchmarks` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/04-worker-service...split/05-benchmarks?expand=1 |

## Title

```
bench: performance and latency harnesses
```

## Description

---

Benchmark harnesses and the `perf:*` scripts. They import repositories/services directly, below any public API, so they measure the storage layer.

**Check:**
- `examples/producer.ts` is **deleted** here and `example:producer` points at a missing file until PR14.
- Five generated markdown reports come along with this. They read as scratch notes — say the word and I'll drop them.
- The two `.sh` files are committed without the executable bit.
