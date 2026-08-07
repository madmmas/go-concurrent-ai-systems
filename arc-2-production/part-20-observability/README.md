# Part 20 — part-20-observability

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part -1:** [`compare/part--1...part-20`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part--1...part-20)

## What this code does

In-process metrics: throughput, latency percentiles (p50/p95/p99), error rates by type. The foundation for Prometheus and OpenTelemetry integration.

## Run it

```bash
cd arc-2-production/part-20-observability
go run ./cmd/news-processor -articles=20 -workers=5 -error-rate=0.2
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
