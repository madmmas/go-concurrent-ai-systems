# Part 18 — Goroutine Leaks

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 17:** [`compare/part-17...part-18`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-17...part-18)

## What this code does

Goroutine leak detection using runtime.NumGoroutine(). Shows a leaky pipeline and the correct buffered-channel fix. Explains how to use pprof goroutine profiles in production.

## Run it

```bash
cd arc-2-production/part-18-goroutine-leaks
go run ./cmd/news-processor -articles=10 -workers=3
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
