# Part 12 — errgroup and Structured Concurrency

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 11:** [`compare/part-11...part-12`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-11...part-12)

## What this code does

errgroup pattern: run N concurrent tasks, return first error, cancel all siblings automatically. Includes a self-contained errgroup implementation using only the standard library.

## Run it

```bash
cd arc-2-production/part-12-errgroup
go run ./cmd/news-processor -articles=3 -workers=2

# Failure demo — first 503 cancels siblings immediately
go run ./cmd/news-processor -articles=3 -workers=2 -error-rate=1.0
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
