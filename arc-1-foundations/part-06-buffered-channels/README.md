# Part 06 — Buffered vs Unbuffered Channels

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Read the post:** [Part 6 — Buffered vs Unbuffered Channels](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-6-buffered-channels/)
> **Diff from Part 5:** [`compare/part-05...part-06`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-05...part-06)

## What this code does

Provides two pipeline implementations for direct comparison:

- **`UnbufferedPipeline`** — every worker send blocks until the collector receives.
  Creates tight synchronisation; slow collectors naturally throttle fast producers.

- **`BufferedPipeline`** — workers send up to `BufferSize` results without blocking.
  Decouples producers from consumers; workers keep running while the collector catches up.

## Run it

```bash
cd arc-1-foundations/part-06-buffered-channels

# Unbuffered — each send waits for the collector
go run ./cmd/news-processor -mode=unbuffered -articles=10

# Buffered — buffer=1 (near-synchronous)
go run ./cmd/news-processor -mode=buffered -articles=10 -buffer=1

# Buffered — buffer=10 (workers rarely block on send)
go run ./cmd/news-processor -mode=buffered -articles=10 -buffer=10
```

## What to observe

With simulated LLM latency, timing differences are small — the bottleneck is the
sleep, not the channel. The real difference shows when the collector does slow work
(database writes, API calls). Buffered channels let workers keep processing
while the collector catches up.

## Key concepts introduced

**Buffered channel:** `make(chan T, n)` — allows n sends without a receiver ready.

**`select` statement:** waits on multiple channels, proceeds with whichever fires first.
Used throughout Parts 8 and 9 for timeout enforcement and cancellation:
```go
select {
case <-time.After(latency):
    return nil       // normal completion
case <-ctx.Done():
    return ctx.Err() // cancelled or timed out
}
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=3s -run='^$'
```
