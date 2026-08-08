# Part 11 — Multi-Stage Pipeline

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 10:** [`compare/part-10...part-11`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-10...part-11)

## What this code does

Four-stage concurrent pipeline: scrape → clean → embed → summarise. Each stage has its own worker count tuned to its specific bottleneck — more scrapers than LLM callers.

## Run it

```bash
cd arc-2-production/part-11-pipeline-stages
go run ./cmd/news-processor -articles=10 -scrape-workers=10 -llm-workers=3
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
