# Part 17 — Token Streaming

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 2 — Production Concurrent AI Systems
> **Diff from Part 16:** [`compare/part-16...part-17`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-16...part-17)

## What this code does

Token-level LLM response streaming via channels. Tokens arrive incrementally and consumers process them before the full response completes, matching real SSE and WebSocket streaming.

## Run it

```bash
cd arc-2-production/part-17-token-streaming
go run ./cmd/news-processor -articles=2 -workers=2
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
