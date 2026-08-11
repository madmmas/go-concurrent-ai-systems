# Part 14 — Rate Limiting

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 13:** [`compare/part-13...part-14`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-13...part-14)

## What this code does

Token bucket rate limiter: acquire a token before each LLM call, blocking when the bucket is empty. Prevents 429 errors before they happen — proactive rather than reactive.

## Run it

```bash
cd arc-2-production/part-14-rate-limiting
go run ./cmd/news-processor -articles=5 -workers=5 -rate=3
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
