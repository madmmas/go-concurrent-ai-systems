# Production-Grade Concurrent AI Systems in Go

Source code for the blog series of the same name.

Each part has its own runnable Go module. The system evolves from a naive
sequential pipeline in Part 1 into a production-grade concurrent AI platform.

## Quick start

```bash
git clone https://github.com/madmmas/go-concurrent-ai-systems
cd go-concurrent-ai-systems

# Run Part 1 — sequential baseline
cd arc-1-foundations/part-01-sequential
go run ./cmd/news-processor

# Run Part 10 — fan-out / fan-in (Arc 2)
cd ../../arc-2-production/part-10-fan-out-fan-in
go run ./cmd/news-processor -articles=6 -workers=3

# Run all tests
cd ../..
go test ./arc-1-foundations/... ./arc-2-production/... -race -timeout 300s
```

Requires Go 1.22 or later. No external dependencies. No API keys.

## Navigating the repo

| Arc | Topic | Status |
|-----|-------|--------|
| [Arc 1 — Concurrency Foundations](./arc-1-foundations/) | Goroutines through graceful shutdown | ✅ Complete |
| [Arc 2 — Production Concurrent AI](./arc-2-production/) | Fan-out, retries, circuit breakers, streaming, RAG | ✅ Complete |
| Arc 3 — Cloud-Native Distributed AI | Kafka, Kubernetes, distributed workflows | 🔜 Planned |
| Arc 4 — Cost-Efficient AI Platform | Token budgets, multi-model routing, caching | 🔜 Planned |

## How to follow the code evolution

Each part is tagged in git:

```bash
git checkout part-01   # exactly Part 1
git checkout part-10   # exactly Part 10
```

Compare between parts:
```
https://github.com/madmmas/go-concurrent-ai-systems/compare/part-01...part-02
```

Commit messages are written as teaching material — read `git log` as a narrative.

## Series posts — Arc 1: Concurrency Foundations

- [Part 1 — Why Concurrency Matters](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-1-sequential-ai-pipeline/)
- [Part 2 — Goroutines and WaitGroups](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-2-goroutines-and-waitgroups/)
- [Part 3 — Race Conditions and Mutexes](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-3-race-conditions-and-mutexes/)
- [Part 4 — Deadlocks](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-4-deadlocks/)
- [Part 5 — Channels and Message Passing](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-5-channels-and-message-passing/)
- [Part 6 — Buffered vs Unbuffered Channels](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-6-buffered-channels/)
- [Part 7 — Worker Pools and Bounded Concurrency](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-7-worker-pools-and-bounded-concurrency/)
- [Part 8 — Context and Timeouts](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-8-context-timeouts/)
- [Part 9 — Cancellation and Graceful Shutdown](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-9-cancellation-shutdown/)

## Series posts — Arc 2: Production Concurrent AI

*(posts being written — code is ready)*

- Part 10 — Fan-Out / Fan-In
- Part 11 — Multi-Stage Pipeline
- Part 12 — errgroup and Structured Concurrency
- Part 13 — Retries and Exponential Backoff
- Part 14 — Rate Limiting
- Part 15 — Circuit Breaker
- Part 16 — Backpressure and Bounded Channels
- Part 17 — Token Streaming
- Part 18 — Goroutine Leaks
- Part 19 — Concurrent RAG Pipeline
- Part 20 — Observability
