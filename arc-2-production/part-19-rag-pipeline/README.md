# Part 19 — Concurrent RAG Pipeline

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 18:** [`compare/part-18...part-19`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-18...part-19)

## What this code does

Full concurrent RAG pipeline: chunk → embed → collector → generate. As soon as all chunks for an article are embedded, that article moves to the generator (completion order, not input order). Fan-out, stage isolation, and backpressure from earlier Arc 2 parts show up in the stage wiring.

## Run it

```bash
cd arc-2-production/part-19-rag-pipeline
go run ./cmd/news-processor -articles=4 -chunks=3
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
