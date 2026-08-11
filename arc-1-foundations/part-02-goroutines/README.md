# Part 02 — Goroutines and WaitGroup

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 1 — Concurrency Foundations
> **Read the post:** [Part 2 — Goroutines and WaitGroups](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-2-goroutines-and-waitgroups/)
> **Diff from Part 1:** [`compare/part-01...part-02`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-01...part-02)

## What this code does

Introduces goroutines to the pipeline. Articles are now processed in parallel —
total time collapses from ~30s to ~2s for 10 articles.

Two broken demos ship alongside the correct processor:
- `no-waitgroup` — naive `go processArticle()` with no WaitGroup; returns in µs
- `loop-capture` — goroutines close over the loop variable; many see the last article
- `processor.go` — correct version with `sync.WaitGroup` (still has the Part 3 race)

## Run it

```bash
# See the correct concurrent version
go run ./cmd/news-processor

# Broken: missing WaitGroup (finishes in µs)
go run ./cmd/broken -mode=no-waitgroup

# Broken: loop-variable capture
go run ./cmd/broken -mode=loop-capture

# Observe the data race introduced in this part
go run -race ./cmd/news-processor
```

## What to observe

- Articles complete out of order — fastest finishes first
- Total time ≈ slowest single article, not the sum
- `-race` reports a data race on the results slice — fixed in Part 3

## Key changes from Part 1

```go
// Part 1 — sequential
for _, article := range articles {
    result := processArticle(article)
    results = append(results, result)
}

// Part 2 — concurrent
var wg sync.WaitGroup
for _, article := range articles {
    wg.Add(1)
    go func(a Article) {
        defer wg.Done()
        result := processArticle(a)
        results = append(results, result) // DATA RACE — fixed in Part 3
    }(article)
}
wg.Wait()
```

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race   # observe the race detector firing
```
