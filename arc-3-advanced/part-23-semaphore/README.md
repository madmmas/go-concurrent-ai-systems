# Part 23 — Semaphore Pattern

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 3 — Advanced Go Concurrency Engineering
> **Diff from Part 22:** [`compare/part-22...part-23`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-22...part-23)

## What this code does

Channel-based semaphore limits concurrent access to a code section inside long-lived worker goroutines. Used when a provider has a concurrency limit (not just a rate limit): 20 workers process articles, only 5 can call the embed API simultaneously.

## Run it

```bash
cd arc-3-advanced/part-23-semaphore
go run ./cmd/news-processor -articles=8 -workers=10 -embed-slots=2
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$'
```
