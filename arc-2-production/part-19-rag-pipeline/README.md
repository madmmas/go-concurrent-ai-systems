# Part 19 — part-19-rag-pipeline

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part -1:** [`compare/part--1...part-19`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part--1...part-19)

## What this code does

Full concurrent RAG pipeline: chunk → embed → generate. Applies fan-out, stage isolation, rate limiting, and backpressure in one production-grade system. Flagship part of Arc 2.

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
