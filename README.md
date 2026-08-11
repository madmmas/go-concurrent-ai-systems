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
go run ./cmd/news-processor -articles=3 -workers=2

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

Each part is tagged in git as a **progressive teaching snapshot** (Part N includes
Parts 1…N only, with blog-aligned code):

```bash
git checkout part-01   # exactly Part 1
git checkout part-10   # Parts 1–10
git checkout main      # full series tip
```

Compare consecutive parts:
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
- [Part 6 — Buffered vs Unbuffered Channels](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-6-buffered-vs-unbuffered-channels/)
- [Part 7 — Worker Pools and Bounded Concurrency](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-7-worker-pools-and-bounded-concurrency/)
- [Part 8 — Context and Timeouts](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-8-context-timeouts/)
- [Part 9 — Cancellation and Graceful Shutdown](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-9-cancellation-shutdown/)

## Series posts — Arc 2: Production Concurrent AI

- [Part 10 — Fan-Out / Fan-In](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-10-fan-out-fan-in/)
- [Part 11 — Multi-Stage Pipeline](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-11-pipeline-stages/)
- [Part 12 — errgroup and Structured Concurrency](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-12-errgroup-structured-concurrency/)
- [Part 13 — Retries and Exponential Backoff](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-13-retries-exponential-backoff/)
- [Part 14 — Rate Limiting](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-14-rate-limiting/)
- [Part 15 — Circuit Breaker](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-15-circuit-breaker/)
- [Part 16 — Backpressure and Bounded Channels](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-16-backpressure/)
- [Part 17 — Token Streaming](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-17-token-streaming/)
- [Part 18 — Goroutine Leaks](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-18-goroutine-leaks/)
- [Part 19 — Concurrent RAG Pipeline](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-19-rag-pipeline/)
- [Part 20 — Observability](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-2-production-concurrent-ai/part-20-observability/)
