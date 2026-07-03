# Part 01 — The Sequential Baseline

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 1 — Concurrency Foundations
> **Read the post:** _link when published_
> **Diff from previous part:** this is the starting point

---

## What this code does

Processes a batch of news articles through a three-stage AI pipeline — summarization, sentiment analysis, and keyword extraction — one article at a time, sequentially.

Every AI call is simulated with random latency (500 ms – 1500 ms by default), matching realistic LLM API response times without requiring network access or API keys.

## Run it

```bash
cd arc-1-foundations/part-01-sequential
go run ./cmd/news-processor

# process more articles
go run ./cmd/news-processor -articles=50
```

## What to observe

Watch the terminal output carefully. Notice:

- Article 2 never starts until article 1 completes all three tasks
- The total time printed at the end is roughly `n × 3 × ~1s`
- The scaling projection table shows what this means at 10 000 articles

That waiting time — that dead space between tasks — is the problem Part 2 fixes.

## Run the tests

```bash
# unit tests (fast simulator, completes in seconds)
go test ./internal/... -v

# race detector — should report nothing for sequential code
go test ./internal/... -race

# benchmarks — saves a baseline you can compare against Part 2
go test ./benchmarks/... -bench=. -benchmem -benchtime=3s | tee benchmarks/baseline.txt
```

## Try breaking it

These experiments build intuition before Part 2:

1. **Increase the article count** — run with `-articles=100` and watch the time grow linearly
2. **Read `TestProcessAll_DurationScalesLinearly`** — this test will fail when we swap in a concurrent processor in Part 2. That failure is the point.
3. **Read `TestProcessAll_SequentialOrdering`** — ordering is guaranteed here. Ask yourself: will that still be true after we add goroutines?

## Package structure

```
part-01-sequential/
├── cmd/news-processor/     entry point, CLI flag, output formatting
├── internal/
│   ├── model/              Article and AIResult types
│   ├── simulator/          LLM call simulator (DefaultConfig, FastConfig)
│   └── pipeline/           Processor — the sequential implementation
│       ├── processor.go
│       └── processor_test.go
└── benchmarks/
    └── benchmark_test.go   scaling curve across 1–100 articles
```

## Key design decisions

**Why simulate LLM calls?**
Real API calls require keys, cost money, have rate limits, and produce non-deterministic latency. The simulator gives every reader identical, reproducible behaviour and keeps the series self-contained.

**Why `FastConfig` vs `DefaultConfig`?**
Unit tests use `FastConfig` (10–50 ms) so the test suite runs in seconds, not minutes. The demo binary uses `DefaultConfig` (500–1500 ms) so the output feels like a real AI pipeline. Benchmarks use a tight 5–15 ms window to generate stable numbers with reasonable wall time.

**Why is `ProcessAll` a method on `Processor`?**
In Part 2 we will add a `ConcurrentProcessor` with the same `ProcessAll` signature. Having both behind a struct makes it easy to swap implementations and compare them directly.
