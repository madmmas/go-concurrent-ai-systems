# Production-Grade Concurrent AI Systems in Go

Source code for the blog series of the same name.

Each part of the series has its own runnable module. The system evolves from a naive sequential pipeline in Part 1 into a distributed, cloud-native AI platform by the end of Arc 5.

## Navigating the repo

| Arc | Topic | Status |
|-----|-------|--------|
| [Arc 1 — Concurrency Foundations](./arc-1-foundations/) | Goroutines, channels, worker pools, race conditions | 🟡 In progress |
| Arc 2 — Production Concurrent AI | Fan-out/fan-in, backpressure, circuit breakers | 🔜 Planned |
| Arc 3 — Cloud-Native Distributed AI | Kafka, Kubernetes, distributed workers | 🔜 Planned |
| Arc 4 — Cost-Efficient AI Platform | GPU scheduling, token budgets, multi-model routing | 🔜 Planned |
| Arc 5 — Advanced AI Runtime | Control planes, AI gateways, chaos engineering | 🔜 Planned |

## Quick start

```bash
git clone https://github.com/moinuddin/go-concurrent-ai-systems
cd go-concurrent-ai-systems

# run Part 1
cd arc-1-foundations/part-01-sequential
go run ./cmd/news-processor

# run all tests across the repo
go test ./...
```

Requires Go 1.22 or later. No external dependencies, no API keys.

## How to follow the code evolution

Each part is tagged in git. To see exactly what changed between parts:

```bash
# see the state of the code at Part 1
git checkout part-01

# compare Part 1 to Part 2 (once published)
# https://github.com/moinuddin/go-concurrent-ai-systems/compare/part-01...part-02
```

Commit messages are written as teaching material — read `git log` as a narrative, not a changelog.

## Series posts

- Part 1 — The Sequential Baseline _(link when published)_
