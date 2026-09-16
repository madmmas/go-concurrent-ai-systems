// Package benchmarks measures the three mutex patterns from Part 3.
//
// Three comparisons:
//
//  1. BadLock vs Safe       — shows the throughput cost of over-locking
//  2. Safe vs Cached        — shows RWMutex cache hit benefit (all unique URLs)
//  3. CachedWithDuplicates  — shows RWMutex benefit when URLs repeat
//
// Run:
//
//	go test ./benchmarks/... -bench=. -benchmem -benchtime=2s -run='^$'
package benchmarks

import (
	"testing"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-03-race-conditions/internal/simulator"
)

// BenchmarkBadLock — mutex wraps the LLM call — throughput ≈ sequential.
func BenchmarkBadLock(b *testing.B) {
	proc := pipeline.NewBadLock(simulator.New(simulator.FastConfig))
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc.ProcessAll(arts)
	}
}

// BenchmarkSafeLock — mutex wraps only the append — full concurrency.
func BenchmarkSafeLock(b *testing.B) {
	proc := pipeline.New(simulator.New(simulator.FastConfig))
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc.ProcessAll(arts)
	}
}

// BenchmarkRWMutex_AllUnique — every article has a unique URL, no cache hits.
// Compares RWMutex overhead vs plain Mutex under write-heavy conditions.
func BenchmarkRWMutex_AllUnique(b *testing.B) {
	proc := pipeline.NewCached(simulator.New(simulator.FastConfig))
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		proc.ProcessAll(arts)
	}
}

// BenchmarkRWMutex_WithCacheHits — 10 articles, only 3 unique URLs.
// 7 of 10 articles are cache hits — served without LLM calls.
// RLock allows all 7 cache-hit goroutines to read simultaneously.
func BenchmarkRWMutex_WithCacheHits(b *testing.B) {
	arts := pipeline.GenerateArticlesWithDuplicates(10, 3)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Recreate processor each iteration so cache starts empty
		proc := pipeline.NewCached(simulator.New(simulator.FastConfig))
		proc.ProcessAll(arts)
	}
}
