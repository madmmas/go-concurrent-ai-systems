#!/usr/bin/env bash
# git-commits-arc1.sh
#
# Replays the full Arc 1 commit history.
# Safe to re-run: skips commits with nothing staged and existing tags.
#
# Fresh repo:
#   git init
#   git remote add origin https://github.com/madmmas/go-concurrent-ai-systems.git
#   bash scripts/git-commits-arc1.sh

set -euo pipefail

# Skip empty commits and duplicate tags so the script can resume after a failure.
git() {
  if [[ "${1:-}" == "commit" ]]; then
    shift
    if ! command git diff --cached --quiet; then
      command git commit "$@"
    fi
  elif [[ "${1:-}" == "tag" && "${2:-}" == "-a" && -n "${3:-}" ]]; then
    if command git rev-parse "$3" >/dev/null 2>&1; then
      return 0
    fi
    command git "$@"
  else
    command git "$@"
  fi
}

# Stage only paths that exist — git add fails the whole command if any pathspec is missing.
git_add_existing() {
  local path staged=0
  for path in "$@"; do
    if [[ -e "$path" ]]; then
      command git add "$path"
      staged=1
    fi
  done
  if [[ "$staged" -eq 0 ]]; then
    echo "error: none of the paths exist: $*" >&2
    return 1
  fi
}

echo "=== Replaying Arc 1 — Go Concurrency Foundations ==="

# ─────────────────────────────────────────────────────────────────────────────
# REPO SCAFFOLD
# ─────────────────────────────────────────────────────────────────────────────

git_add_existing go.work .gitignore README.md .github/ scripts/
git commit -m "chore: scaffold repo for Production-Grade Concurrent AI Systems in Go

go.work links all Arc 1 modules as a single workspace.
CI runs tests + race detector across all parts on push.
Arc table in README tracks planned arcs 2-4."

# ─────────────────────────────────────────────────────────────────────────────
# ARC 1 README
# ─────────────────────────────────────────────────────────────────────────────

git add arc-1-foundations/README.md
git commit -m "docs: add Arc 1 overview with evolution table

Maps all 9 parts, their core concepts, and the progression from
~30s sequential to graceful-shutdown concurrent pipeline."

# ─────────────────────────────────────────────────────────────────────────────
# PART 01 — Sequential Baseline
# ─────────────────────────────────────────────────────────────────────────────

git add arc-1-foundations/part-01-sequential/go.mod
git add arc-1-foundations/part-01-sequential/internal/model/
git commit -m "feat(part-01): add Article and AIResult types

The two core data structures for the entire series.
Article flows in; AIResult flows out.
Isolated in their own package to avoid circular imports."

git add arc-1-foundations/part-01-sequential/internal/simulator/
git commit -m "feat(part-01): add LLM simulator with DefaultConfig and FastConfig

DefaultConfig (500-1500ms) matches real LLM latency for demos.
FastConfig (10-50ms) keeps the test suite running in seconds.
Mutex on the RNG makes it safe for concurrent use in later parts."

git add arc-1-foundations/part-01-sequential/internal/pipeline/processor.go
git commit -m "feat(part-01): implement sequential article processor

Three AI tasks per article, one article at a time.
This is intentionally the wrong design.

Watch the output: article 2 never starts until article 1
finishes all three tasks. That idle wait is what Parts 2-7 fix.

Total time for 10 articles: ~30 seconds."

git add arc-1-foundations/part-01-sequential/internal/pipeline/processor_test.go
git commit -m "test(part-01): add three tests that encode the claims of Part 1

  TestProcessAll_CorrectResults        — baseline correctness
  TestProcessAll_SequentialOrdering    — results in input order
  TestProcessAll_DurationScalesLinearly — time grows with article count

The last two WILL FAIL in Part 2. That failure is the lesson."

git add arc-1-foundations/part-01-sequential/cmd/ \
        arc-1-foundations/part-01-sequential/benchmarks/ \
        arc-1-foundations/part-01-sequential/README.md
git commit -m "feat(part-01): add CLI, benchmarks, and README

-articles flag lets readers vary the load.
Scaling projection table is computed from actual measured duration.
README has 'try breaking it' experiments to run before Part 2."

git tag -a part-01 -m "Part 1: Sequential baseline — measure the problem before solving it"

# ─────────────────────────────────────────────────────────────────────────────
# PART 02 — Goroutines and WaitGroup
# ─────────────────────────────────────────────────────────────────────────────

git add arc-1-foundations/part-02-goroutines/go.mod
git add arc-1-foundations/part-02-goroutines/internal/model/
git add arc-1-foundations/part-02-goroutines/internal/simulator/
git commit -m "feat(part-02): add module, copy model and simulator from Part 1"

git add arc-1-foundations/part-02-goroutines/internal/pipeline/broken.go
git commit -m "feat(part-02): add naive goroutine version — intentionally broken

  go processArticle(article)

Result: 'Finished in 85µs'. All work was discarded.
main() exits before goroutines complete. Compiler won't catch it.
Understanding why this fails is more valuable than the fix."

git add arc-1-foundations/part-02-goroutines/internal/pipeline/processor.go
git commit -m "feat(part-02): fix goroutine discard with sync.WaitGroup

Three additions:
  wg.Add(1)       — track goroutine before launch
  defer wg.Done() — signal completion (defer fires even on panic)
  wg.Wait()       — block until all done

Total time: ~3s for 10 articles vs ~30s in Part 1.
A data race remains on the results slice. Run with -race to observe it."

git add arc-1-foundations/part-02-goroutines/internal/pipeline/processor_test.go
git commit -m "test(part-02): document the changed concurrent contract

  TestProcessAll_CorrectResults         — all results despite parallelism
  TestProcessAll_OrderingNotGuaranteed  — results are unordered by design
  TestProcessAll_DurationSublinear      — 10 articles < 5× one article"

git add arc-1-foundations/part-02-goroutines/cmd/ \
        arc-1-foundations/part-02-goroutines/benchmarks/ \
        arc-1-foundations/part-02-goroutines/README.md
git commit -m "feat(part-02): add CLI, benchmarks, README

README shows the exact diff from Part 1: 8 lines, 90% time reduction.
Run with -race to see the data race we leave for Part 3 to fix."

git tag -a part-02 -m "Part 2: Goroutines and WaitGroup — concurrent but not yet safe"

# ─────────────────────────────────────────────────────────────────────────────
# PART 03 — Race Conditions and Mutexes
# ─────────────────────────────────────────────────────────────────────────────

git add arc-1-foundations/part-03-race-conditions/go.mod
git add arc-1-foundations/part-03-race-conditions/internal/model/
git add arc-1-foundations/part-03-race-conditions/internal/simulator/
git commit -m "feat(part-03): add module, upgrade simulator to mutex-protected RNG

Multiple workers calling the same LLMClient concurrently caused a
race on math/rand's *Rand state. Mutex wraps only the RNG read,
not the sleep — no contention during the actual latency wait."

git add arc-1-foundations/part-03-race-conditions/broken/
git commit -m "feat(part-03): add broken race condition demo

  go run -race ./broken

Shows the DATA RACE warning and demonstrates how the expected
result count varies non-deterministically between runs.
Race conditions are dangerous precisely because they pass locally
and fail unpredictably under production load."

git add arc-1-foundations/part-03-race-conditions/internal/pipeline/processor.go
git commit -m "feat(part-03): fix race condition with sync.Mutex

The fix is two lines:
  mu.Lock()
  results = append(results, result)
  mu.Unlock()

Critical: the mutex wraps only the append, not the LLM call.
Locking around processArticle() serialises everything:
  3.4s → 32.8s for 10 articles. Same complexity, worse than Part 1."

git add arc-1-foundations/part-03-race-conditions/internal/pipeline/processor_test.go
git commit -m "test(part-03): verify correctness, race safety, and lock scope

  TestProcessAll_CorrectResultCount          — mutex gives consistent results
  TestProcessAll_NoRaceUnderHighConcurrency  — 50 goroutines, race-clean
  TestProcessAll_MutexDoesNotSerialize       — proves lock scope is correct

TestProcessAll_MutexDoesNotSerialize is the most important test here.
It catches the mistake of locking too wide."

git add arc-1-foundations/part-03-race-conditions/cmd/ \
        arc-1-foundations/part-03-race-conditions/benchmarks/ \
        arc-1-foundations/part-03-race-conditions/README.md
git commit -m "feat(part-03): add CLI, benchmarks, README"

git tag -a part-03 -m "Part 3: Race conditions and mutexes — correct and concurrent"

# ─────────────────────────────────────────────────────────────────────────────
# PART 04 — Deadlocks
# ─────────────────────────────────────────────────────────────────────────────

git add arc-1-foundations/part-04-deadlocks/go.mod
git add arc-1-foundations/part-04-deadlocks/internal/
git commit -m "feat(part-04): add deadlock-safe pipeline

Implements two versions of ProcessAll:
  - mutex-based (from Part 3)
  - channel-based (preview of Part 5)

Both demonstrate the two rules that prevent deadlock:
  Rule 1: unbuffered send needs a concurrent receiver
  Rule 2: always close(ch) after last send, use defer"

git add arc-1-foundations/part-04-deadlocks/deadlocks/send-no-receive/ \
        arc-1-foundations/part-04-deadlocks/deadlocks/circular-wait/ \
        arc-1-foundations/part-04-deadlocks/deadlocks/forgotten-close/
git commit -m "feat(part-04): add three intentional deadlock demos

  go run ./deadlocks/send-no-receive   → fatal: all goroutines asleep
  go run ./deadlocks/circular-wait     → fatal: all goroutines asleep
  go run ./deadlocks/forgotten-close   → hangs after 3 articles, then fatal

Each demo shows the Go runtime's deadlock message and goroutine state.
Reading 'goroutine N [chan send]' or '[chan receive]' tells you exactly
which goroutine is stuck and on what operation."

git add arc-1-foundations/part-04-deadlocks/deadlocks/correct/
git commit -m "feat(part-04): add correct version showing all three fixes

  Fix 1: sender in goroutine so receiver can start first
  Fix 2: separate goroutines break the circular wait
  Fix 3: defer close(ch) guarantees channel close on any exit path"

git add arc-1-foundations/part-04-deadlocks/cmd/ \
        arc-1-foundations/part-04-deadlocks/README.md
git commit -m "feat(part-04): add CLI and README

CLI intentionally does NOT run the deadlock demos — they crash.
README explains how to run each demo and what output to expect."

git tag -a part-04 -m "Part 4: Deadlocks — three patterns, two rules"

# ─────────────────────────────────────────────────────────────────────────────
# PART 05 — Channels and Message Passing
# ─────────────────────────────────────────────────────────────────────────────

git add arc-1-foundations/part-05-channels/go.mod
git add arc-1-foundations/part-05-channels/internal/
git commit -m "feat(part-05): replace mutex with channel-based result collection

The shared results slice and mutex are gone.
Workers send into resultsCh; a single collector owns the slice.
Only one goroutine ever writes to results — no mutex needed.

Architecture:
  Workers → resultsCh → single collector → results slice

The closer goroutine (wg.Wait → close) is load-bearing:
without it, the range loop blocks forever (Part 4's forgotten-close)."

git add arc-1-foundations/part-05-channels/cmd/ \
        arc-1-foundations/part-05-channels/benchmarks/ \
        arc-1-foundations/part-05-channels/README.md
git commit -m "feat(part-05): add CLI, benchmarks, README

README ends with the next problem:
'One goroutine per article does not scale to 100,000 articles.
See Part 7 for the fix.'
Part 6 explains buffered channels first."

git tag -a part-05 -m "Part 5: Channels — no shared memory, no mutex, single-owner collection"

# ─────────────────────────────────────────────────────────────────────────────
# PART 06 — Buffered vs Unbuffered Channels
# ─────────────────────────────────────────────────────────────────────────────

git add arc-1-foundations/part-06-buffered-channels/go.mod
git add arc-1-foundations/part-06-buffered-channels/internal/
git commit -m "feat(part-06): add buffered vs unbuffered channel comparison

Two processors for direct comparison:
  UnbufferedPipeline — every send blocks until collector receives
  BufferedPipeline   — workers queue up to BufferSize results without blocking

Key insight: buffered channels decouple producers from consumers.
Buffer capacity is the backpressure knob — too large hides slowdowns,
too small throttles producers unnecessarily.

Also introduces the select statement explicitly:
  select waits on multiple channels, proceeds with the first ready one.
  Used for timeout enforcement (Part 8) and cancellation (Part 9)."

git add arc-1-foundations/part-06-buffered-channels/cmd/ \
        arc-1-foundations/part-06-buffered-channels/benchmarks/ \
        arc-1-foundations/part-06-buffered-channels/README.md
git commit -m "feat(part-06): add CLI with -mode flag and benchmarks

  go run ./cmd/news-processor -mode=unbuffered -articles=10
  go run ./cmd/news-processor -mode=buffered -articles=10 -buffer=1
  go run ./cmd/news-processor -mode=buffered -articles=10 -buffer=10"

git tag -a part-06 -m "Part 6: Buffered channels — decouple producers from consumers"

# ─────────────────────────────────────────────────────────────────────────────
# PART 07 — Worker Pools
# ─────────────────────────────────────────────────────────────────────────────

git add arc-1-foundations/part-07-worker-pools/go.mod
git add arc-1-foundations/part-07-worker-pools/internal/
git commit -m "feat(part-07): implement bounded worker pool

Problem with Part 5: one goroutine per article.
At 100,000 articles: 100,000 goroutines, memory spike, provider rate limits.

Worker pool decouples concurrency from input size.
Workers is a number you control regardless of article count.

  jobs channel feeds all articles in
  W workers drain the jobs channel
  results channel collects output
  close(jobs) signals workers to exit when drained

Timing comparison with -workers flag:
  workers=1:  sequential (~63s for 20 articles)
  workers=5:  5× faster  (~13s)
  workers=20: maximum    (~4s, bounded by slowest article)"

git add arc-1-foundations/part-07-worker-pools/cmd/ \
        arc-1-foundations/part-07-worker-pools/benchmarks/ \
        arc-1-foundations/part-07-worker-pools/README.md
git commit -m "feat(part-07): add CLI with -workers flag and worker-count benchmarks

go run ./cmd/news-processor -articles=20 -workers=1
go run ./cmd/news-processor -articles=20 -workers=5
go run ./cmd/news-processor -articles=20 -workers=20"

git tag -a part-07 -m "Part 7: Worker pools — bounded concurrency, production-ready"

# ─────────────────────────────────────────────────────────────────────────────
# PART 08 — Context and Timeouts
# ─────────────────────────────────────────────────────────────────────────────

git add arc-1-foundations/part-08-context-timeouts/go.mod
git add arc-1-foundations/part-08-context-timeouts/internal/
git commit -m "feat(part-08): add context.WithTimeout to worker pool

Every LLM call now takes a context.Context.
Per-article timeouts prevent one hung provider from blocking a worker.

Two context levels:
  pipeline context — passed to ProcessAll by the caller
  article context  — child of pipeline, timeout per article

Child contexts inherit parent deadlines: if the pipeline context
is cancelled, all article contexts cancel immediately.

AIResult.Err carries the outcome — no article is silently dropped.
Every article produces exactly one result, success or failure.

Simulator FailureProfile adds TimeoutRate for realistic failure injection:
  DefaultConfig    — no failures (matches Parts 1-7 behaviour)
  UnreliableConfig — 20% timeout rate, 500-3000ms latency

Rule: every context.WithTimeout must have a matching defer cancel()."

git add arc-1-foundations/part-08-context-timeouts/cmd/ \
        arc-1-foundations/part-08-context-timeouts/README.md
git commit -m "feat(part-08): add CLI with -unreliable flag

  go run ./cmd/news-processor -articles=10 -workers=5 -timeout=4s
  go run ./cmd/news-processor -articles=10 -workers=5 -timeout=2s -unreliable"

git tag -a part-08 -m "Part 8: Context and timeouts — every call needs a deadline"

# ─────────────────────────────────────────────────────────────────────────────
# PART 09 — Cancellation and Graceful Shutdown
# ─────────────────────────────────────────────────────────────────────────────

git add arc-1-foundations/part-09-cancellation/go.mod
git add arc-1-foundations/part-09-cancellation/internal/
git commit -m "feat(part-09): add graceful shutdown with ShutdownReport

signal.NotifyContext wires SIGTERM/SIGINT to context cancellation.
Workers detect pipeline context cancellation and drain remaining
jobs from the queue — reporting them as Queued rather than dropping them.

ShutdownReport invariant:
  Succeeded + Failed + Cancelled + Queued == total input articles

This holds under normal completion, early cancellation, and SIGTERM.
No article is ever silently lost.

This pattern is exactly how Kubernetes rolling deployments work:
SIGTERM arrives, context cancels, in-flight work completes or times out,
queued work is reported as unprocessed for the next deployment to resume."

git add arc-1-foundations/part-09-cancellation/cmd/ \
        arc-1-foundations/part-09-cancellation/README.md
git commit -m "feat(part-09): add CLI with -cancel-after flag

  go run ./cmd/news-processor -articles=10 -workers=3
  go run ./cmd/news-processor -articles=10 -workers=3 -cancel-after=1s
  # Then press Ctrl+C at any point to trigger real SIGTERM"

git tag -a part-09 -m "Part 9: Cancellation and graceful shutdown — stopping safely"
git tag -a arc-1-complete -m "Arc 1 complete: Go Concurrency Foundations — 9 parts"

echo ""
echo "=== Arc 1 commits and tags complete ==="
git tag -l | grep -E "^(part|arc)" || true
echo ""
echo "Push with: git push origin main --tags"
