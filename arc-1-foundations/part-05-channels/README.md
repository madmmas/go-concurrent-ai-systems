# Part 04 — Channels and Message Passing

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Diff from Part 3:** `github.com/moinuddin/go-concurrent-ai-systems/compare/part-03...part-04`

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

// Part 4 — channel
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
