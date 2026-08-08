# Part 13 — Retries and Exponential Backoff

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 12:** [`compare/part-12...part-13`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-12...part-13)

## What this code does

Exponential backoff with jitter for retryable errors (429 rate limit, 503 server error). Dead letter channel for articles that exhaust all retry attempts.

## Run it

```bash
cd arc-2-production/part-13-retries
go run ./cmd/news-processor -articles=10 -workers=3 -rate-limit=0.3
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
