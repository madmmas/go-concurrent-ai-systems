# Arc 1 — Go Concurrency Foundations

Nine parts. One evolving pipeline. The news platform starts as a single sequential
`for` loop and ends with a context-aware, gracefully-stoppable worker pool.

| Part | Folder | Core concept | Key lesson |
|------|--------|--------------|------------|
| 1 | `part-01-sequential` | Sequential baseline | Measure the problem before solving it |
| 2 | `part-02-goroutines` | Goroutines + WaitGroup | Broken first — three bugs in a row |
| 3 | `part-03-race-conditions` | Mutex | Lock the minimum critical section |
| 4 | `part-04-deadlocks` | Deadlock patterns | Three ways to deadlock, two rules to prevent them |
| 5 | `part-05-channels` | Unbuffered channels | Single-owner collection eliminates shared state |
| 6 | `part-06-buffered-channels` | Buffered channels + select | Decouple producer from consumer; three select patterns |
| 7 | `part-07-worker-pools` | Worker pool | Decouple concurrency from input size |
| 8 | `part-08-context-timeouts` | context.WithTimeout | Every external call needs a deadline |
| 9 | `part-09-cancellation` | Graceful shutdown | Account for every article, even under cancellation |

## Pipeline evolution

```
Part 1        Part 2        Part 3        Part 4
Sequential  → Goroutines  → Mutex fix   → Deadlock
~30s/10art    ~3.5s          ~3.4s         patterns

Part 5        Part 6        Part 7        Part 8        Part 9
Channels    → Buffered    → Worker     → Timeouts   → Graceful
no mutex      + select      pool          per-article   shutdown
                            any N workers deadline      ShutdownReport
```

## What each part adds

**Part 1** establishes the sequential baseline. The timing table makes the problem real: 10 articles in 30 seconds, 100 articles in 5 minutes.

**Part 2** introduces goroutines and immediately shows three failure modes in order: a silently discarded goroutine, a loop-capture bug, and a data race. WaitGroup fixes the first two. The race remains deliberately for Part 3.

**Part 3** fixes the data race with a mutex — then proves, with a real 3.4s vs 32.8s comparison, that locking in the wrong place destroys every gain concurrency gave us.

**Part 4** demonstrates three classic deadlock patterns (send-no-receive, circular-wait, forgotten-close) and shows how Go's runtime message identifies the stuck goroutine by operation and line number.

**Part 5** replaces the mutex with a channel. One goroutine owns the results slice. No shared memory, no lock to get wrong.

**Part 6** shows when to use buffered vs unbuffered channels and introduces `select` — three patterns: timeout enforcement, non-blocking probe, and cancellation — the primitives used throughout Parts 8 and 9.

**Part 7** decouples concurrency from article count. The worker pool runs the same code with 1, 5, or 20 workers; the speedup is measured, not assumed.

**Part 8** adds `context.WithTimeout` to every LLM call. Per-article deadlines, two-level context hierarchy, mandatory `defer cancel()`.

**Part 9** wires OS signal handling via `signal.NotifyContext`. The `ShutdownReport` invariant — `Succeeded + Failed + Cancelled + Queued == total` — holds under SIGTERM, panic, and mid-flight cancellation.

## Run everything

```bash
# From repo root
go test ./arc-1-foundations/... -race -timeout 300s

# Benchmarks (main branch only in CI, any time locally)
for part in part-01-sequential part-06-buffered-channels \
            part-07-worker-pools part-08-context-timeouts \
            part-09-cancellation; do
  echo "=== $part ==="
  (cd arc-1-foundations/$part && \
    go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$')
done
```

## Series posts

- [Part 1 — Why Concurrency Matters](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-1-sequential-ai-pipeline/)
- [Part 2 — Goroutines and WaitGroups](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-2-goroutines-and-waitgroups/)
- [Part 3 — Race Conditions and Mutexes](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-3-race-conditions-and-mutexes/)
- [Part 4 — Deadlocks](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-4-deadlocks/)
- [Part 5 — Channels and Message Passing](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-5-channels-and-message-passing/)
- [Part 6 — Buffered vs Unbuffered Channels](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-6-buffered-vs-unbuffered-channels/)
- [Part 7 — Worker Pools and Bounded Concurrency](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-7-worker-pools-and-bounded-concurrency/)
- [Part 8 — Context and Timeouts](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-8-context-timeouts/)
- [Part 9 — Cancellation and Graceful Shutdown](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-9-cancellation-shutdown/)
