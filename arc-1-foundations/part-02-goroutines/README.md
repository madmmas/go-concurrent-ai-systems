# Part 02 — Goroutines and WaitGroup

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Diff from Part 1:** `github.com/moinuddin/go-concurrent-ai-systems/compare/part-01...part-02`

## What this code does

Introduces goroutines to the pipeline. Articles are now processed in parallel —
total time collapses from ~30s to ~2s for 10 articles.

Two versions ship:
- `broken.go` — naive `go processArticle()` with no WaitGroup; main exits before work completes
- `processor.go` — correct version with `sync.WaitGroup`

## Run it

```bash
# See the correct concurrent version
go run ./cmd/news-processor

# See the broken version (exits in ~100µs, work never completes)
go run -v ./internal/pipeline/

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
