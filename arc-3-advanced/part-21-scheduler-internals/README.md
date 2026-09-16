# Part 21 — Go Scheduler Internals

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 3 — Advanced Go Concurrency Engineering
> **Diff from Part 20:** [`compare/part-20...part-21`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-20...part-21)

## What this code does

Demonstrates the M:P:G model through benchmarks: IO-bound workloads scale beyond GOMAXPROCS workers because goroutines yield their P during network waits. CPU-bound workloads plateau at GOMAXPROCS. Run the GOMAXPROCS sweep benchmark to see the difference.

## Run it

```bash
cd arc-3-advanced/part-21-scheduler-internals
go run ./cmd/news-processor -mode=io -workers=8 -articles=8
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$'
```
