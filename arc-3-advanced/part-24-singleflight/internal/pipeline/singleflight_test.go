package pipeline_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-24-singleflight/internal/simulator"
)

// TestSingleflight_OnlyOneCallPerKey verifies that concurrent requests
// for the same key result in exactly one fn() execution.
func TestSingleflight_OnlyOneCallPerKey(t *testing.T) {
	var g pipeline.Group
	var calls, started int64
	const concurrent = 20

	var wg sync.WaitGroup
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddInt64(&started, 1)
			g.Do("same-key", func() (interface{}, error) {
				atomic.AddInt64(&calls, 1)
				// Hold the call open until every goroutine has arrived.
				for atomic.LoadInt64(&started) < concurrent {
					time.Sleep(time.Millisecond)
				}
				time.Sleep(20 * time.Millisecond)
				return "result", nil
			})
		}()
	}
	wg.Wait()

	if calls != 1 {
		t.Errorf("fn() called %d times for %d concurrent requests, want 1", calls, concurrent)
	}
}

// TestSingleflight_PanicDoesNotHangFollowers verifies that waiters get a
// PanicError instead of blocking forever when fn panics.
func TestSingleflight_PanicDoesNotHangFollowers(t *testing.T) {
	var g pipeline.Group
	go func() {
		defer func() { recover() }() // the leader still panics — contain it
		g.Do("k", func() (interface{}, error) {
			time.Sleep(30 * time.Millisecond)
			panic("embedding decoder blew up")
		})
	}()
	time.Sleep(5 * time.Millisecond) // let the leader start

	done := make(chan error, 1)
	go func() {
		_, err, _ := g.Do("k", func() (interface{}, error) { return 1, nil })
		done <- err
	}()
	select {
	case err := <-done:
		var pe *pipeline.PanicError
		if !errors.As(err, &pe) {
			t.Errorf("follower err = %v, want *PanicError", err)
		}
	case <-time.After(time.Second):
		t.Fatal("follower still blocked 1s after leader panicked")
	}
}

// TestSingleflight_DifferentKeysRunConcurrently verifies different keys
// do not block each other.
func TestSingleflight_DifferentKeysRunConcurrently(t *testing.T) {
	var g pipeline.Group
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		key := "key-" + string(rune('A'+i))
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			g.Do(k, func() (interface{}, error) {
				time.Sleep(20 * time.Millisecond)
				return k, nil
			})
		}(key)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// 5 different keys running concurrently should finish in ~20ms, not 100ms
	if elapsed > 80*time.Millisecond {
		t.Errorf("different keys did not run concurrently: took %v", elapsed)
	}
}

// TestEmbeddingCache_DeduplicatesLLMCalls verifies that concurrent requests
// for the same URL result in one LLM call, not many.
func TestEmbeddingCache_DeduplicatesLLMCalls(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 30 * time.Millisecond,
		MaxLatency: 50 * time.Millisecond,
		Failure:    simulator.DefaultProfile,
	}
	pool := pipeline.New(simulator.New(cfg), 20, 2*time.Second)

	// 20 articles all with the same URL — should produce 1 LLM embed call
	arts := pipeline.GenerateArticlesWithDuplicates(20, 1)
	results, _ := pool.ProcessAll(context.Background(), arts)

	if len(results) != 20 {
		t.Fatalf("expected 20 results, got %d", len(results))
	}

	hits, shared, calls := pool.CacheStats()
	t.Logf("llm-calls=%d shared=%d cache-hits=%d (1 URL, 20 articles)", calls, shared, hits)
	if calls != 1 {
		t.Errorf("made %d embedding LLM calls for 1 URL, want 1", calls)
	}

	// All 20 should have embeddings (either computed or shared)
	for _, r := range results {
		if r.Err != nil {
			continue
		}
		if len(r.Embedding) == 0 {
			t.Errorf("article %d: missing embedding", r.ArticleID)
		}
	}
}

// TestEmbeddingCache_LeaderTimeoutDoesNotFailFollowers verifies the shared
// call is not tied to the deadline of whichever caller arrived first.
func TestEmbeddingCache_LeaderTimeoutDoesNotFailFollowers(t *testing.T) {
	llm := simulator.New(simulator.Config{MinLatency: 100 * time.Millisecond, MaxLatency: 100 * time.Millisecond})
	c := pipeline.NewEmbeddingCache(llm)

	leaderCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	leaderErr := make(chan error, 1)
	go func() { _, err := c.GetEmbedding(leaderCtx, "u", 1); leaderErr <- err }()
	time.Sleep(5 * time.Millisecond)

	emb, err := c.GetEmbedding(context.Background(), "u", 2) // no deadline
	if err != nil {
		t.Fatalf("follower failed with %v — leader's deadline leaked into shared call", err)
	}
	if len(emb) == 0 {
		t.Error("follower got empty embedding")
	}
	if err := <-leaderErr; !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("leader err = %v, want its own DeadlineExceeded", err)
	}
}

// TestEmbeddingCache_FollowerCanGiveUp verifies a waiter returns on its own
// deadline instead of waiting for the shared call to finish.
func TestEmbeddingCache_FollowerCanGiveUp(t *testing.T) {
	llm := simulator.New(simulator.Config{MinLatency: 200 * time.Millisecond, MaxLatency: 200 * time.Millisecond})
	c := pipeline.NewEmbeddingCache(llm)
	go c.GetEmbedding(context.Background(), "u", 1)
	time.Sleep(5 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.GetEmbedding(ctx, "u", 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Errorf("follower waited %v; should give up at its 20ms deadline", d)
	}
}

// TestPool_AllResultsDelivered verifies correctness with unique URLs.
func TestPool_AllResultsDelivered(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 4, 2*time.Second)
	arts := pipeline.GenerateArticles(10)
	results, _ := pool.ProcessAll(context.Background(), arts)
	if len(results) != 10 {
		t.Errorf("expected 10, got %d", len(results))
	}
}

// TestPool_NoRace verifies no data races under the race detector.
func TestPool_NoRace(t *testing.T) {
	pool := pipeline.New(simulator.New(simulator.FastConfig), 8, 2*time.Second)
	arts := pipeline.GenerateArticlesWithDuplicates(30, 5)
	results, _ := pool.ProcessAll(context.Background(), arts)
	if len(results) != 30 {
		t.Errorf("expected 30, got %d", len(results))
	}
}
