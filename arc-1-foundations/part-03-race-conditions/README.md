# Part 03 — Race Conditions and Mutexes

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 1 — Concurrency Foundations
> **Read the post:** [Part 3 — Race Conditions and Mutexes](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-3-race-conditions-and-mutexes/)
> **Diff from Part 2:** [`compare/part-02...part-03`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-02...part-03)

## What this code does

Fixes the data race from Part 2. A `sync.Mutex` protects the shared results
slice — only one goroutine appends at a time. The mutex wraps only the append,
not the LLM call, preserving concurrency.

## Run it

```bash
# See the race condition first
go run -race ./broken

# Good lock — mutex around append only (~3.4s for 10 articles)
go run ./cmd/news-processor -mode=good

# Bad lock — mutex around LLM calls (~32s for 10 articles)
go run ./cmd/news-processor -mode=bad

# Race detector stays silent for both good and bad
go run -race ./cmd/news-processor -mode=good
```

## Key changes from Part 2

```go
// Part 2 — data race
results = append(results, result)

// Part 3 — mutex protected
mu.Lock()
results = append(results, result)
mu.Unlock()
```

**Critical:** the mutex wraps only the append. Not the LLM call. Locking
around the LLM call would re-serialize the pipeline — all the concurrency
gains from Part 2 would disappear.

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race     # race detector must stay silent
go test ./internal/... -race -count=10   # run repeatedly to confirm stability
```
