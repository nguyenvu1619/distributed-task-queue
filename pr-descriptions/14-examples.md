# PR14 — examples: rewrite on the public API

| | |
|---|---|
| **base** | `split/13-client-facade` |
| **head** | `split/14-examples` |
| **open** | https://github.com/nguyenvu1619/distributed-task-queue/compare/split/13-client-facade...split/14-examples?expand=1 |

## Title

```
examples: rewrite on the public API
```

## Description

---

`producer.ts` and `worker.ts` move onto `TaskQueue`; `transactional-publish.ts` shows the outbox-free pattern.

**Check:**
- `examples/` joins the typecheck project, so a broken example now fails `npm run typecheck` instead of rotting.
- Benchmark imports move from `../src/index` to deep paths because PR13 removed those symbols from the public surface — intended, not a workaround.
- `example:producer` works again after being broken since PR05.
