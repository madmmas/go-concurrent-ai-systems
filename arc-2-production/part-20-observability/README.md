# Part 20 — Observability

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 19:** [`compare/part-19...part-20`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-19...part-20)

## What this code does

In-process metrics: throughput (articles/sec), latency percentiles (p50/p95/p99), and error breakdown by type. The foundation for Prometheus and OpenTelemetry integration.

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
