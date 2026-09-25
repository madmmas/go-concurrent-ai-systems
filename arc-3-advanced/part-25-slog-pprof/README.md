# Part 25 — Structured Logging and Production Diagnostics

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 3 — Advanced Go Concurrency Engineering
> **Diff from Part 24:** [`compare/part-24...part-25`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-24...part-25)

## What this code does

Replaces fmt.Printf with slog (Go 1.21) — including the LLM simulator's call logging — so every line carries article_id, stage, latency_ms and worker_id as structured key-value pairs. Adds a time.Ticker metric flusher (with a final flush on Stop) and a pprof endpoint on a dedicated, localhost-bound mux for live goroutine and heap profiling.

## Run it

```bash
cd arc-3-advanced/part-25-slog-pprof
go run ./cmd/news-processor -articles=10 -log=json -level=debug

# live profiling
go run ./cmd/news-processor -articles=200 -workers=20 -pprof=localhost:6060
go tool pprof -top http://localhost:6060/debug/pprof/goroutine
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$'
```
