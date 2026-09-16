# Part 26 — OpenTelemetry Traces

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 3 — Advanced Go Concurrency Engineering
> **Diff from Part 25:** [`compare/part-25...part-26`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-25...part-26)

## What this code does

End-to-end distributed tracing using an OTEL-compatible tracer implemented with stdlib. Each article produces a trace with child spans per stage (summarise, embed). The upgrade path to real OTEL SDK and Jaeger requires only changing the constructor.

## Run it

```bash
cd arc-3-advanced/part-26-opentelemetry
go run ./cmd/news-processor -articles=5 -workers=3
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$'
```
