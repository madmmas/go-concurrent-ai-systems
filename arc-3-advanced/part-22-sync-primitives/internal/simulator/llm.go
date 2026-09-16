// Package simulator provides LLM client simulation for Arc 3.
// Inherits all Arc 2 failure modes; adds call counting for cost tracking.
package simulator

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrRateLimit  = errors.New("llm: rate limit exceeded (429)")
	ErrServerError = errors.New("llm: server error (503)")
)

func IsRetryable(err error) bool {
	return errors.Is(err, ErrRateLimit) || errors.Is(err, ErrServerError)
}

type FailureProfile struct {
	RateLimitRate float64
	ServerErrRate float64
}

var DefaultProfile = FailureProfile{}
var ProductionProfile = FailureProfile{RateLimitRate: 0.10, ServerErrRate: 0.05}

type Config struct {
	MinLatency time.Duration
	MaxLatency time.Duration
	Failure    FailureProfile
}

var DefaultConfig = Config{
	MinLatency: 200 * time.Millisecond,
	MaxLatency: 800 * time.Millisecond,
	Failure:    DefaultProfile,
}

var FastConfig = Config{
	MinLatency: 5 * time.Millisecond,
	MaxLatency: 20 * time.Millisecond,
	Failure:    DefaultProfile,
}

// LLMClient simulates an AI API. Safe for concurrent use.
// Arc 3 adds atomic call and token counters for cost attribution.
type LLMClient struct {
	cfg    Config
	mu     sync.Mutex
	rng    *rand.Rand
	calls  int64 // atomic
	tokens int64 // atomic — simulated token count
}

func New(cfg Config) *LLMClient {
	return &LLMClient{
		cfg: cfg,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (c *LLMClient) Call(ctx context.Context, task string, articleID int) error {
	c.mu.Lock()
	spread := int64(c.cfg.MaxLatency - c.cfg.MinLatency + 1)
	latency := c.cfg.MinLatency + time.Duration(c.rng.Int63n(spread))
	r := c.rng.Float64()
	c.mu.Unlock()

	atomic.AddInt64(&c.calls, 1)
	// Simulate token usage: 100-500 tokens per call
	c.mu.Lock()
	tokensThisCall := int64(100 + c.rng.Intn(400))
	c.mu.Unlock()
	atomic.AddInt64(&c.tokens, tokensThisCall)

	fp := c.cfg.Failure
	if r < fp.RateLimitRate {
		fmt.Printf("  [%d] %s → 429 rate limited\n", articleID, task)
		return ErrRateLimit
	}
	r -= fp.RateLimitRate
	if r < fp.ServerErrRate {
		fmt.Printf("  [%d] %s → 503 server error\n", articleID, task)
		return ErrServerError
	}

	fmt.Printf("  [%d] %s started (%v)\n", articleID, task, latency.Round(time.Millisecond))
	select {
	case <-time.After(latency):
		fmt.Printf("  [%d] %s completed\n", articleID, task)
		return nil
	case <-ctx.Done():
		fmt.Printf("  [%d] %s cancelled\n", articleID, task)
		return ctx.Err()
	}
}

// TotalCalls returns total API calls made since creation.
func (c *LLMClient) TotalCalls() int64 { return atomic.LoadInt64(&c.calls) }

// TotalTokens returns simulated tokens consumed since creation.
func (c *LLMClient) TotalTokens() int64 { return atomic.LoadInt64(&c.tokens) }
