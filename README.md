# Production-Grade Concurrent AI Systems in Go

Source code for the blog series of the same name.

Each part has its own runnable Go module. The system evolves from a naive
sequential pipeline in Part 1 into a production-grade concurrent platform
across five planned arcs.

## Quick start

```bash
git clone https://github.com/madmmas/go-concurrent-ai-systems
cd go-concurrent-ai-systems

# Run Part 1 — sequential baseline
cd arc-1-foundations/part-01-sequential
go run ./cmd/news-processor

# Run all tests across Arc 1
cd arc-1-foundations
go test ./... -race
```

Requires Go 1.22 or later. No external dependencies. No API keys.

## Navigating the repo

| Arc | Topic | Status |
|-----|-------|--------|
| [Arc 1 — Concurrency Foundations](./arc-1-foundations/) | Goroutines through graceful shutdown | ✅ Complete |
| Arc 2 — Production Concurrent AI | Fan-out, retries, circuit breakers, streaming | 🔜 Planned |
| Arc 3 — Cloud-Native Distributed AI | Kafka, Kubernetes, distributed workflows | 🔜 Planned |
| Arc 4 — Cost-Efficient AI Platform | Token budgets, multi-model routing, caching | 🔜 Planned |

## How to follow the code evolution

Each part is tagged in git:

```bash
git checkout part-01   # see Part 1 exactly
git checkout part-04   # see Part 4 exactly
```

To see what changed between two parts:
```
https://github.com/madmmas/go-concurrent-ai-systems/compare/part-01...part-02
```

Commit messages are written as teaching material — read `git log` as a narrative.

## Series posts — Arc 1

- [Part 1 — Why Concurrency Matters](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-1-sequential-ai-pipeline/)
- [Part 2 — Goroutines and WaitGroups](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-2-goroutines-and-waitgroups/)
- [Part 3 — Race Conditions and Mutexes](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-3-race-conditions-and-mutexes/)
- [Part 4 — Deadlocks](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-4-deadlocks/)
- [Part 5 — Channels and Message Passing](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-5-channels-and-message-passing/)
- [Part 6 — Buffered vs Unbuffered Channels](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-6-buffered-vs-unbuffered-channels/)
- [Part 7 — Worker Pools and Bounded Concurrency](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-7-worker-pools-and-bounded-concurrency/)
- [Part 8 — Context and Timeouts](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-8-context-and-timeouts/)
- [Part 9 — Cancellation and Graceful Shutdown](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-9-cancellation-and-graceful-shutdown/)
