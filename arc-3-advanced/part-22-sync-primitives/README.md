# Part 22 — sync.Once, sync.Map, sync.Pool, and atomic

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 3 — Advanced Go Concurrency Engineering
> **Diff from Part 21:** [`compare/part-21...part-22`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-21...part-22)

## What this code does

Covers the four missing sync primitives: sync.Once for singleton LLM client initialisation, sync.Map for concurrent article dedup cache, sync.Pool for AIResult object reuse, and the full atomic API (Add/Load/Store/CompareAndSwap) for lock-free counters.

## Run it

```bash
cd arc-3-advanced/part-22-sync-primitives
go run ./cmd/news-processor -articles=9 -workers=3 -unique-urls=3
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$'
```
