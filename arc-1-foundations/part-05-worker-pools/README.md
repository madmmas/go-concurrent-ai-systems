# Part 05 — Worker Pools and Bounded Concurrency

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Diff from Part 4:** `github.com/moinuddin/go-concurrent-ai-systems/compare/part-04...part-05`

## What this code does

Replaces unbounded goroutine-per-article with a fixed worker pool. The number
of concurrent goroutines is controlled regardless of article count.

## Run it

```bash
# Experiment with worker count
go run ./cmd/news-processor -articles=20 -workers=1    # sequential
go run ./cmd/news-processor -articles=20 -workers=5    # typical
go run ./cmd/news-processor -articles=20 -workers=20   # maximum
```

## Key changes from Part 4

```go
// Part 4 — one goroutine per article (unbounded)
for _, article := range articles {
    go func(a Article) { ... }(article)
}

// Part 5 — fixed worker pool (bounded)
jobs := make(chan Article, len(articles))

for w := 1; w <= p.Workers; w++ {
    go func(id int) {
        for article := range jobs {   // worker processes many articles
            result := processArticle(article)
            resultsCh <- result
        }
    }(w)
}

for _, article := range articles {
    jobs <- article
}
close(jobs)
```

## What to observe

- `-workers=1` behaves like Part 1 — sequential, slow
- `-workers=5` gives near-maximum throughput with controlled resource use
- Adding workers beyond article count gives no further speedup
- Worker count is the rate-limit knob — lower it to respect LLM quotas

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=3s
```
