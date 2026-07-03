# Arc 1 — Go Concurrency Foundations

The complete foundation arc. Builds one evolving system — an AI news pipeline —
from naive sequential code to a production-ready concurrent design with timeouts
and graceful shutdown.

| Part | Folder | Core concept | Key teaching |
|------|--------|--------------|--------------|
| 1 | `part-01-sequential` | Sequential baseline | Measure before optimising |
| 2 | `part-02-goroutines` | Goroutines + WaitGroup | Broken first, then fixed |
| 3 | `part-03-race-conditions` | Mutex | Lock the minimum critical section |
| 4 | `part-04-deadlocks` | Deadlock patterns | Three ways to deadlock, two rules to prevent them |
| 5 | `part-05-channels` | Unbuffered channels | Single-owner collection eliminates shared state |
| 6 | `part-06-buffered-channels` | Buffered channels + select | Decouple producers from consumers |
| 7 | `part-07-worker-pools` | Worker pool | Decouple concurrency from input size |
| 8 | `part-08-context-timeouts` | context.WithTimeout | Every external call needs a deadline |
| 9 | `part-09-cancellation` | Graceful shutdown | Account for every article, even under cancellation |

## Evolution

```
Part 1       Part 2       Part 3       Part 4       Part 5
Sequential → Goroutines → Mutex fix → Deadlock   → Channels
~30s/10art   ~3s/10art    ~3s/10art    patterns     no mutex

Part 6       Part 7       Part 8       Part 9
Buffered  → Worker    → Timeouts  → Graceful
channels    pool        per-article  shutdown
                        deadline     ShutdownReport
```

## Run everything

```bash
# From repo root
go test ./arc-1-foundations/... -race -timeout 300s
```
