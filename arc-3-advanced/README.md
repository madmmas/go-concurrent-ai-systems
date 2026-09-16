# Arc 3 — Advanced Go Concurrency Engineering

Fills the concurrency tool gaps from Arcs 1 and 2.
No external infrastructure required — every part runs with `go run`.

| Part | Folder | Core concept | Key lesson |
|------|--------|--------------|------------|
| 21 | `part-21-scheduler-internals` | M:P:G, GOMAXPROCS | IO-bound scales beyond NumCPU; CPU-bound does not |
| 22 | `part-22-sync-primitives` | Once, Map, Pool, atomic | The four missing sync primitives with news platform use cases |
| 23 | `part-23-semaphore` | Channel semaphore | Limit concurrent access to a code section, not goroutine count |
| 24 | `part-24-singleflight` | Singleflight | Deduplicate concurrent calls with the same key |
| 25 | `part-25-slog-pprof` | slog + Ticker + pprof | Structured logging, periodic flush, live profiling |
| 26 | `part-26-opentelemetry` | OTEL tracing | Distributed traces across goroutines, upgrade-ready for Jaeger |

## Pattern evolution

```
Part 21          Part 22          Part 23
Scheduler      → sync.Once      → Semaphore
M:P:G            sync.Map         channel-based
GOMAXPROCS       sync.Pool        concurrency limit
IO vs CPU        atomic CAS       per code section

Part 24          Part 25          Part 26
Singleflight   → slog+pprof    → OpenTelemetry
deduplicate      structured       spans + traces
concurrent       logging          upgrade path
requests         + Ticker         to Jaeger/Tempo
```

## Run everything

```bash
# From repo root — each part is its own module
for part in arc-3-advanced/part-*/; do
  (cd "$part" && go test ./... -race -timeout 180s)
done

# Benchmarks
for part in arc-3-advanced/part-*/; do
  echo "=== $(basename $part) ==="
  (cd "$part" && go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$')
done
```

## External dependencies

| Part | Package | Purpose |
|------|---------|---------|
| 23 | stdlib only | Channel semaphore shown first |
| 24 | stdlib only | Singleflight implemented inline |
| 25 | `log/slog`, `net/http/pprof` | Both stdlib since Go 1.21 |
| 26 | stdlib only | OTEL-compatible tracer implemented inline |

All 6 parts compile and test without any `go get`. The inline implementations
mirror the real package APIs exactly — replacing them with `golang.org/x/sync`
or `go.opentelemetry.io/otel` requires only changing constructor calls.
