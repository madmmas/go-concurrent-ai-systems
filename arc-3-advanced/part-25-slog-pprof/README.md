# Part 25 — Structured Logging and Production Diagnostics

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 3 — Advanced Go Concurrency Engineering
> **Diff from Part 24:** [`compare/part-24...part-25`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-24...part-25)

## What this code does

Replaces fmt.Printf with slog (Go 1.21): every log line carries article_id, stage, latency_ms, and worker_id as structured key-value pairs. Adds time.Ticker for periodic metric flush and the pprof HTTP endpoint for live goroutine and heap profiling.

## Run it

```bash
cd arc-3-advanced/part-25-slog-pprof
go run ./cmd/news-processor -articles=10 -log=json
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$'
```
