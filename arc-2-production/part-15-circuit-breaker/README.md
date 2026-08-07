# Part 15 — part-15-circuit-breaker

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part -1:** [`compare/part--1...part-15`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part--1...part-15)

## What this code does

Three-state circuit breaker: Closed → Open → HalfOpen. Fails fast when provider is unhealthy, probes recovery with a single call, re-opens on probe failure.

## Run it

```bash
cd arc-2-production/part-15-circuit-breaker
go run ./cmd/news-processor -articles=20 -workers=3 -error-rate=0.5
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
