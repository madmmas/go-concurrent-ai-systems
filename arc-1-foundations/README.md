# Arc 1 — Go Concurrency Foundations

This arc builds the mental model for concurrent programming in Go through a
single evolving project — an AI-powered news processing pipeline.

| Part | Topic | Key concept introduced |
|------|-------|----------------------|
| [Part 01](./part-01-sequential/) | Sequential baseline | Why concurrency matters |
| [Part 02](./part-02-goroutines/) | Goroutines + WaitGroup | Concurrency, loop capture bug |
| [Part 03](./part-03-race-conditions/) | Race conditions + Mutex | Data races, critical sections |
| [Part 04](./part-04-channels/) | Channels | Message passing, single ownership |
| [Part 05](./part-05-worker-pools/) | Worker pools | Bounded concurrency, rate limiting |

## The evolution

```
Part 1          Part 2          Part 3          Part 4          Part 5
Sequential  →  Goroutines  →  Mutex fix   →  Channels    →  Worker Pool
~30s/10art     ~2s/10art      ~2s/10art      ~2s/10art      configurable
correct        fast+buggy     correct+fast   cleaner         production-ready
```

## Run everything

```bash
# From repo root
go test ./arc-1-foundations/... -race
```
