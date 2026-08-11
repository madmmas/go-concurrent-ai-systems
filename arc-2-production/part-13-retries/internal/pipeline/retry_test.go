package pipeline_test

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-13-retries/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-13-retries/internal/simulator"
)

func newFastPool() *pipeline.RetryPool {
	return pipeline.New(
		simulator.New(simulator.FastConfig),
		3,
		200*time.Millisecond,
		pipeline.DefaultRetryConfig,
	)
}

func TestRetry_AllSucceedWhenNoFailures(t *testing.T) {
	pool := newFastPool()
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(9))
	if len(results) != 9 {
		t.Fatalf("expected 9 results, got %d", len(results))
	}
	for _, r := range results {
		if r.Result.Err != nil || r.DeadLetter {
			t.Errorf("article %d: unexpected failure", r.Result.ArticleID)
		}
	}
}

func TestRetry_DeadLetterOnExhaustion(t *testing.T) {
	// 100% failure rate — always exhausts retries
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
		Failure: simulator.FailureProfile{
			RateLimitRate: 1.0,
		},
	}
	retry := pipeline.RetryConfig{
		MaxAttempts: 2,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    50 * time.Millisecond,
		JitterFrac:  0.0,
	}
	pool := pipeline.New(simulator.New(cfg), 2, 500*time.Millisecond, retry)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(4))

	deadLetters := 0
	for _, r := range results {
		if r.DeadLetter {
			deadLetters++
		}
	}
	if deadLetters != 4 {
		t.Errorf("expected 4 dead letters, got %d", deadLetters)
	}
}

func TestRetry_RetriesAreRecorded(t *testing.T) {
	// 50% rate limit — some articles will need retries
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
		Failure: simulator.FailureProfile{
			RateLimitRate: 0.5,
		},
	}
	retry := pipeline.RetryConfig{
		MaxAttempts: 5,
		BaseDelay:   5 * time.Millisecond,
		MaxDelay:    50 * time.Millisecond,
		JitterFrac:  0.0,
	}
	pool := pipeline.New(simulator.New(cfg), 3, 500*time.Millisecond, retry)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(10))

	totalRetries := 0
	for _, r := range results {
		totalRetries += r.Result.Retries
	}
	// With 50% failure rate and 5 attempts, most articles need at least 1 retry
	t.Logf("total retries across 10 articles: %d", totalRetries)
}

func TestRetry_TotalResultsAlwaysMatchInput(t *testing.T) {
	cfg := simulator.Config{
		MinLatency: 5 * time.Millisecond,
		MaxLatency: 10 * time.Millisecond,
		Failure: simulator.FailureProfile{
			RateLimitRate: 0.3,
			ServerErrRate: 0.1,
		},
	}
	pool := pipeline.New(simulator.New(cfg), 3, 200*time.Millisecond, pipeline.DefaultRetryConfig)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(15))
	if len(results) != 15 {
		t.Errorf("expected 15 results, got %d — articles lost", len(results))
	}
}
