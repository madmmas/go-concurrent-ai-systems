# Part 16 — Backpressure and Bounded Channels

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 15:** [`compare/part-15...part-16`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-15...part-16)

## What this code does

A bounded jobs channel applies backpressure: the producer blocks when the queue is full, naturally throttling to match consumer speed with no explicit signalling.

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
