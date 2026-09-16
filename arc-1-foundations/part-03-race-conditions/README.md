# Part 03 — Race Conditions and Mutexes

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 1 — Concurrency Foundations
> **Read the post:** [Part 3 — Race Conditions and Mutexes](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-3-race-conditions-and-mutexes/)
> **Diff from Part 2:** [`compare/part-02...part-03`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-02...part-03)

## What this code does

Fixes the data race from Part 2 with a `sync.Mutex`, then demonstrates two
more mutex patterns: the bad lock (too wide — re-serialises the pipeline) and
`sync.RWMutex` for read-heavy workloads.

Three processors for comparison:

| Processor | Mutex type | Use case |
|-----------|-----------|----------|
| `SafeProcessor` | `sync.Mutex` | Protects the results slice append — minimum critical section |
| `BadLockProcessor` | `sync.Mutex` | Locks around the LLM call — correct but kills throughput |
| `CachedProcessor` | `sync.RWMutex` | Article cache: many concurrent readers, rare writers |

## Run it

```bash
cd arc-1-foundations/part-03-race-conditions

# See the race condition from Part 2 (before the fix)
go run -race ./broken

# Good lock — mutex around append only (~3.4s for 10 articles)
go run ./cmd/news-processor -mode=good

# Bad lock — mutex around LLM calls (~32s for 10 articles)
go run ./cmd/news-processor -mode=bad

# RWMutex cache — many concurrent readers, few unique URLs
go run ./cmd/news-processor -mode=cache -articles=50
```

## sync.RWMutex — when to use it

`sync.Mutex` serialises all access: only one goroutine at a time, whether
reading or writing. `sync.RWMutex` allows concurrent reads:

```go
// Multiple goroutines can hold RLock simultaneously
mu.RLock()
cached, hit := cache[url]
mu.RUnlock()

// Lock is exclusive — no readers or writers run concurrently
mu.Lock()
cache[url] = result
mu.Unlock()
```

Use `RWMutex` when reads are frequent and concurrent reads are safe.
In the `CachedProcessor`, 45 of 50 articles are cache hits —
45 goroutines check the cache simultaneously holding only `RLock`.

**When not to use it:** if writers are as frequent as readers, the overhead
of `RWMutex` (tracking active readers) outweighs the benefit. Benchmark first.

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$'
```
