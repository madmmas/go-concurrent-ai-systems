# Part 18 — Goroutine Leaks

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 17:** [`compare/part-17...part-18`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-17...part-18)

## What this code does

Goroutine leak detection using runtime.NumGoroutine(). Shows a leaky pipeline and the correct buffered-channel fix. Optional pprof listener for production-style inspection.

## Run it

```bash
cd arc-2-production/part-18-goroutine-leaks

# Fixed pool — no leak
go run ./cmd/news-processor -articles=8 -workers=3

# Leaky pool — early cancel abandons senders
go run ./cmd/news-processor -mode=leaky -articles=8 -workers=3

# Optional pprof
go run ./cmd/news-processor -pprof=:6060 -articles=8 -workers=3
# then: go tool pprof http://localhost:6060/debug/pprof/goroutine
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
