# Part 16 — part-16-backpressure

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part -1:** [`compare/part--1...part-16`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part--1...part-16)

## What this code does

Bounded jobs channel applies backpressure: producer blocks when the queue is full, naturally throttling to match consumer speed without explicit signalling.

## Run it

```bash
cd arc-2-production/part-16-backpressure
go run ./cmd/news-processor -articles=15 -workers=2 -queue=3
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
