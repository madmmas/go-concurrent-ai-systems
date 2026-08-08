# Part 09 — Cancellation and Graceful Shutdown

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Read the post:** [Part 9 — Cancellation and Graceful Shutdown](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-9-cancellation-shutdown/)
> **Diff from Part 8:** [`compare/part-08...part-09`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-08...part-09)

## What this code does

Adds OS signal handling and a `ShutdownReport` that accounts for every article —
even under abrupt cancellation. This is how real Go services handle SIGTERM.

`signal.NotifyContext` wires SIGTERM/SIGINT to context cancellation.
Workers detect pipeline cancellation and drain the jobs queue,
reporting remaining articles as `Queued` rather than silently dropping them.

**ShutdownReport invariant:**
```
Succeeded + Failed + Cancelled + Queued == total input articles
```
This holds under normal completion, early cancel, and SIGTERM.

## Run it

```bash
cd arc-1-foundations/part-09-cancellation

# Normal run — all articles complete
go run ./cmd/news-processor -articles=10 -workers=5

# Simulate SIGTERM after 1 second
go run ./cmd/news-processor -articles=10 -workers=3 -cancel-after=1s

# Press Ctrl+C at any point during a run to trigger real graceful shutdown
go run ./cmd/news-processor -articles=20 -workers=5
```

## What to observe

With `-cancel-after=1s`, the report shows:

```
Succeeded : 2
Failed    : 0 (timeout/error)
Cancelled : 3 (in-flight when shutdown arrived)
Queued    : 5 (never started)
Duration  : 1.000s

Total accounted for: 10 / 10
```

No articles are ever lost. The `Queued` count tells you exactly what to re-process
in the next deployment — the same pattern Kubernetes uses for rolling updates.

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
