# Part 14 — part-14-rate-limiting

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part -1:** [`compare/part--1...part-14`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part--1...part-14)

## What this code does

Token bucket rate limiter: acquire a token before each LLM call, blocking when empty. Prevents 429 errors before they happen.

## Run it

```bash
cd arc-2-production/part-14-rate-limiting
go run ./cmd/news-processor -articles=10 -workers=5 -rate=5
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
