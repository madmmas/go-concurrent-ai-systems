# Part 24 — Singleflight: Deduplicating Concurrent Requests

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 3 — Advanced Go Concurrency Engineering
> **Diff from Part 23:** [`compare/part-23...part-24`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-23...part-24)

## What this code does

When 50 goroutines request an embedding for the same URL simultaneously, singleflight ensures only one LLM call goes out. All others share the result. Includes a self-contained singleflight implementation (Do, DoChan, panic-safe) using only the standard library; the shared call runs on a context detached from any single caller, and each caller can still give up on its own deadline.

## Run it

```bash
cd arc-3-advanced/part-24-singleflight
go run ./cmd/news-processor -articles=20 -workers=20 -unique-urls=1
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$'
```
