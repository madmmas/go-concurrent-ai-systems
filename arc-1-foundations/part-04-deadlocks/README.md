# Part 04 — Deadlocks: When Goroutines Wait Forever

> **Series:** Production-Grade Concurrent AI Systems in Go
> **Arc:** 1 — Concurrency Foundations
> **Read the post:** [Part 4 — Deadlocks](https://madmmasblog.vercel.app/blog/building-concurrent-ai-pipelines-in-go/phase-1-concurrency-fundamentals/part-4-deadlocks/)
> **Diff from Part 3:** [`compare/part-03...part-04`](https://github.com/madmmas/go-concurrent-ai-systems/compare/part-03...part-04)

## What this code does

Demonstrates three classic deadlock patterns, then shows the rules that prevent them.
The main pipeline code (`internal/pipeline`) is deadlock-safe.
The `deadlocks/` folder contains intentionally broken programs — each one crashes with
`fatal error: all goroutines are asleep - deadlock!`

## Run the deadlock demos

⚠️ These programs crash intentionally. That is the point.

```bash
cd arc-1-foundations/part-04-deadlocks

# Demo 1: Send with no receiver — crashes immediately
go run ./deadlocks/send-no-receive

# Demo 2: Circular wait between goroutines — crashes
go run ./deadlocks/circular-wait

# Demo 3: Forgotten channel close — processes 3 articles then hangs
go run ./deadlocks/forgotten-close

# Correct version — all three patterns fixed, runs cleanly
go run ./deadlocks/correct
```

## Run the safe pipeline

```bash
go run ./cmd/news-processor -articles=10
```

## What to observe

In each deadlock demo, read the runtime error message carefully:

```
fatal error: all goroutines are asleep - deadlock!

goroutine 1 [chan send]:
main.main()
    send-no-receive/main.go:27 +0x28
```

`[chan send]` or `[chan receive]` tells you exactly which operation is blocked.
That's Go's runtime pointing directly at the problem.

## The two prevention rules

**Rule 1:** Every unbuffered channel send needs a concurrent receiver.
Put the sender in a goroutine so the receiver can start.

**Rule 2:** The sender closes the channel after the last send. Use `defer close(ch)`.
Without it, any `range` loop over the channel blocks forever.

## Run the tests

```bash
go test ./internal/... -v
go test ./internal/... -race
```
