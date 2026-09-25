# Part 26 — OpenTelemetry Traces

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 3 — Advanced Go Concurrency Engineering
> **Diff from Part 25:** [`compare/part-25...part-26`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-25...part-26)

## What this code does

End-to-end tracing with an OTEL-style tracer implemented with the stdlib. Each batch is one trace: a root `process-batch` span, one `process-article` span per article (created on a worker goroutine from the batch ctx), and `summarise` / `embed` child spans. Finished spans go to an in-memory recorder (waterfall view) and optionally a stdout JSON exporter. The API follows the shape of the OTEL SDK; moving to the real SDK and Jaeger means mechanical call-site changes, listed in the package doc.

## Run it

```bash
cd arc-3-advanced/part-26-opentelemetry
go run ./cmd/news-processor -articles=3 -workers=2            # waterfall
go run ./cmd/news-processor -articles=1 -export=json           # span JSON
go run ./cmd/news-processor -articles=6 -workers=3 -fail=0.2   # error spans
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$'
```
