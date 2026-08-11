# Part 10 — Fan-Out / Fan-In

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 9:** [`compare/part-09...part-10`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-09...part-10)

## What this code does

Runs three AI tasks per article concurrently (fan-out) and collects all results (fan-in). Total per-article time is bounded by the slowest single task, not the sum of all three.

## Run it

```bash
cd arc-2-production/part-10-fan-out-fan-in
go run ./cmd/news-processor -articles=3 -workers=2
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
