# Part 19 — Concurrent RAG Pipeline

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 18:** [`compare/part-18...part-19`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-18...part-19)

## What this code does

Full concurrent RAG pipeline: chunk → embed → generate. The flagship of Arc 2 — applies fan-out, stage isolation, rate limiting, and backpressure together in one production-grade system.

## Run it

```bash
cd arc-2-production/part-19-rag-pipeline
go run ./cmd/news-processor -articles=5 -chunks=4
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
