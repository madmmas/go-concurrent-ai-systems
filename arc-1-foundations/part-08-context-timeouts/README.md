# Part 08 — Context and Timeouts

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Read the post:** [Part 8 — Context and Timeouts](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-8-context-timeouts/)
> **Diff from Part 7:** [`compare/part-07...part-08`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-07...part-08)

## What this code does

Adds `context.WithTimeout` to the worker pool. Every LLM call now receives a context.
A per-article timeout prevents one hung provider call from blocking a worker indefinitely.

Two context levels:
- **Pipeline context** — passed to `ProcessAll` by the caller; controls the whole batch
- **Article context** — child of pipeline, carries a per-article deadline

`AIResult.Err` carries the outcome for timed-out articles — nothing is silently dropped.

## Run it

```bash
cd arc-1-foundations/part-08-context-timeouts

# Healthy pipeline (generous timeout, no failures)
go run ./cmd/news-processor -articles=10 -workers=5 -timeout=5s

# Unreliable provider (20% timeout rate, tight deadline)
go run ./cmd/news-processor -articles=10 -workers=5 -timeout=2s -unreliable
```

## What to observe

With `-unreliable`, some articles will fail with `context deadline exceeded`.
The pipeline still finishes and accounts for every article:

```
Total     : 10 articles
Succeeded : 7
Failed    : 3
Duration  : 2.1s
```

Failed articles are never silently dropped — each one produces an `AIResult`
with `Err` set. The total always equals the input count.

## Key rule

Every `context.WithTimeout` must have a matching `defer cancel()`:
```go
articleCtx, cancel := context.WithTimeout(ctx, p.ArticleTimeout)
defer cancel() // always — releases resources even on success
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
