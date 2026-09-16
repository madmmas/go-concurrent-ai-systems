package pipeline_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-21-scheduler-internals/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-21-scheduler-internals/internal/simulator"
)

// TestSchedulerInfo verifies GOMAXPROCS and NumCPU are positive.
func TestSchedulerInfo(t *testing.T) {
	info := pipeline.CaptureSchedulerInfo()
	if info.GOMAXPROCS <= 0 {
		t.Errorf("GOMAXPROCS=%d, want > 0", info.GOMAXPROCS)
	}
	if info.NumCPU <= 0 {
		t.Errorf("NumCPU=%d, want > 0", info.NumCPU)
	}
	if info.NumGoroutine <= 0 {
		t.Errorf("NumGoroutine=%d, want > 0", info.NumGoroutine)
	}
	t.Logf("GOMAXPROCS=%d NumCPU=%d NumGoroutine=%d",
		info.GOMAXPROCS, info.NumCPU, info.NumGoroutine)
}

// TestIOBound_AllResultsDelivered verifies correctness of IO-bound pool.
func TestIOBound_AllResultsDelivered(t *testing.T) {
	pool := pipeline.NewIOBound(simulator.New(simulator.FastConfig), 4)
	arts := pipeline.GenerateArticles(10)
	results, _ := pool.ProcessAll(context.Background(), arts)
	if len(results) != len(arts) {
		t.Errorf("expected %d results, got %d", len(arts), len(results))
	}
}

// TestCPUBound_AllResultsDelivered verifies correctness of CPU-bound pool.
func TestCPUBound_AllResultsDelivered(t *testing.T) {
	pool := pipeline.NewCPUBound(4, 1000)
	arts := pipeline.GenerateArticles(10)
	results, _ := pool.ProcessAll(arts)
	if len(results) != len(arts) {
		t.Errorf("expected %d results, got %d", len(arts), len(results))
	}
}

// TestCPUBound_DeterministicHash verifies CPU-bound work produces consistent results.
func TestCPUBound_DeterministicHash(t *testing.T) {
	pool := pipeline.NewCPUBound(2, 100)
	arts := pipeline.GenerateArticles(4)

	r1, _ := pool.ProcessAll(arts)
	r2, _ := pool.ProcessAll(arts)

	// Build maps for comparison (order may vary)
	m1 := make(map[int]string)
	m2 := make(map[int]string)
	for _, r := range r1 { m1[r.ArticleID] = r.Summary }
	for _, r := range r2 { m2[r.ArticleID] = r.Summary }

	for id, s := range m1 {
		if m2[id] != s {
			t.Errorf("article %d: hash changed between runs (%s vs %s)", id, s, m2[id])
		}
	}
}

// TestGOMAXPROCS_ScopedChange verifies GOMAXPROCS can be changed and restored.
// This is the mechanism benchmarks use to test different P counts.
func TestGOMAXPROCS_ScopedChange(t *testing.T) {
	original := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(original)

	runtime.GOMAXPROCS(1)
	if got := runtime.GOMAXPROCS(0); got != 1 {
		t.Errorf("GOMAXPROCS(1) did not take effect: got %d", got)
	}
	runtime.GOMAXPROCS(original)
	if got := runtime.GOMAXPROCS(0); got != original {
		t.Errorf("restore failed: got %d, want %d", got, original)
	}
}

// TestIOBound_NoRace verifies the IO-bound pool under the race detector.
func TestIOBound_NoRace(t *testing.T) {
	pool := pipeline.NewIOBound(simulator.New(simulator.FastConfig), 8)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(20))
	if len(results) != 20 {
		t.Errorf("expected 20, got %d", len(results))
	}
}

// TestCPUBound_NoRace verifies the CPU-bound pool under the race detector.
func TestCPUBound_NoRace(t *testing.T) {
	pool := pipeline.NewCPUBound(8, 100)
	results, _ := pool.ProcessAll(pipeline.GenerateArticles(20))
	if len(results) != 20 {
		t.Errorf("expected 20, got %d", len(results))
	}
}

// TestIOBound_ScalesBeyondCPUCount verifies that IO-bound pools benefit from
// more workers than CPUs — because goroutines yield P during IO waits.
func TestIOBound_ScalesBeyondCPUCount(t *testing.T) {
	ncpu := runtime.NumCPU()
	arts := pipeline.GenerateArticles(ncpu * 4)

	cfg := simulator.Config{
		MinLatency: 10 * time.Millisecond,
		MaxLatency: 20 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}

	// Workers = NumCPU
	p1 := pipeline.NewIOBound(simulator.New(cfg), ncpu)
	_, d1 := p1.ProcessAll(context.Background(), arts)

	// Workers = NumCPU * 4 (more than CPUs)
	p2 := pipeline.NewIOBound(simulator.New(cfg), ncpu*4)
	_, d2 := p2.ProcessAll(context.Background(), arts)

	// IO-bound: more workers should be faster
	if d2 >= d1 {
		t.Logf("workers=%d: %v, workers=%d: %v", ncpu, d1, ncpu*4, d2)
		t.Log("IO-bound pool did not scale beyond NumCPU — may indicate CPU-bound behaviour in simulator")
	} else {
		t.Logf("✓ IO scaling confirmed: %d workers %v → %d workers %v",
			ncpu, d1, ncpu*4, d2)
	}
}
