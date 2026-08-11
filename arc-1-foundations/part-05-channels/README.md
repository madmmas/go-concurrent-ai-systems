# Part 05 — Channels and Message Passing

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 1 — Concurrency Foundations
> **Read the post:** [Part 5 — Channels and Message Passing](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-5-channels-and-message-passing/)
> **Diff from Part 4:** [`compare/part-04...part-05`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-04...part-05)

## What this code does

Replaces the mutex with a channel. Worker goroutines send results into a
channel; a single collector owns the results slice. No mutex needed — only
one goroutine ever touches the slice.

## Run it

```bash
go run -race ./cmd/news-processor   # no mutex, no race
```

## Key changes from Part 3

```go
// Part 3 — mutex
mu.Lock()
results = append(results, result)
mu.Unlock()

// Part 5 — channel
resultsCh <- result          // worker sends
for r := range resultsCh {   // single collector receives
    results = append(results, r)
}
```

## What to observe

- No mutex anywhere in the code
- `-race` stays silent — single-owner collection is inherently safe
- Channel must be closed after all workers finish — see the closer goroutine

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
