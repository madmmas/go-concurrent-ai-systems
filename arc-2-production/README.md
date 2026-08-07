# Arc 2 — Production Concurrent AI Systems

Applies Arc 1's concurrency foundations to real production workloads.
Each part introduces one pattern that a production AI pipeline actually needs.

| Part | Folder | Core concept | Key lesson |
|------|--------|--------------|------------|
| 10 | `part-10-fan-out-fan-in` | Fan-out / Fan-in | Per-article tasks run concurrently |
| 11 | `part-11-pipeline-stages` | Multi-stage pipeline | Different concurrency per stage |
| 12 | `part-12-errgroup` | errgroup | First error cancels siblings |
| 13 | `part-13-retries` | Retry + backoff | Exponential backoff + dead letter |
| 14 | `part-14-rate-limiting` | Token bucket | Prevent 429s before they happen |
| 15 | `part-15-circuit-breaker` | Circuit breaker | Fail fast on unhealthy provider |
| 16 | `part-16-backpressure` | Bounded channels | Natural flow control |
| 17 | `part-17-token-streaming` | Token streaming | Incremental LLM response handling |
| 18 | `part-18-goroutine-leaks` | Leak detection | runtime.NumGoroutine + pprof |
| 19 | `part-19-rag-pipeline` | Concurrent RAG | All patterns in one system |
| 20 | `part-20-observability` | Metrics | Throughput, latency percentiles |

## Run everything

```bash
# From repo root
go test ./arc-2-production/... -race -timeout 300s
```
