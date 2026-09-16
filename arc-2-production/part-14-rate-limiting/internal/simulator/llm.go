// Package simulator provides a realistic LLM client simulation for Arc 2.
//
// Arc 2 introduces production failure modes that Arc 1 never had:
//   - Timeouts (introduced Arc 1 Part 8, carried forward)
//   - Rate limits (HTTP 429) — provider tells you to slow down
//   - Hard errors (HTTP 500/503) — provider is broken
//   - Transient errors — work retrying, won't fail permanently
//
// FailureProfile controls the probability of each mode independently.
// Real LLM providers exhibit all of these simultaneously under load.
package simulator

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Sentinel errors — callers inspect these to decide retry strategy.
var (
	ErrRateLimit    = errors.New("llm: rate limit exceeded (429)")
	ErrServerError  = errors.New("llm: server error (503)")
	ErrTimeout      = errors.New("llm: request timed out")
)

// IsRetryable returns true for errors that warrant a retry attempt.
// Rate limits and server errors are retryable; hard errors are not.
func IsRetryable(err error) bool {
	return errors.Is(err, ErrRateLimit) || errors.Is(err, ErrServerError)
}

// FailureProfile controls the probability of each failure mode.
// All rates are in [0.0, 1.0]. They are evaluated independently.
type FailureProfile struct {
	TimeoutRate    float64       // probability call hangs past TimeoutAfter
	TimeoutAfter   time.Duration
	RateLimitRate  float64       // probability call returns 429
	ServerErrRate  float64       // probability call returns 503
}

var DefaultProfile = FailureProfile{
	TimeoutRate:   0.0,
	TimeoutAfter:  5 * time.Second,
	RateLimitRate: 0.0,
	ServerErrRate: 0.0,
}

// ProductionProfile simulates a real LLM provider under moderate load.
// ~10% rate limits, ~5% server errors, ~5% timeouts.
var ProductionProfile = FailureProfile{
	TimeoutRate:   0.05,
	TimeoutAfter:  3 * time.Second,
	RateLimitRate: 0.10,
	ServerErrRate: 0.05,
}

// StressProfile simulates a provider under heavy load.
var StressProfile = FailureProfile{
	TimeoutRate:   0.15,
	TimeoutAfter:  2 * time.Second,
	RateLimitRate: 0.25,
	ServerErrRate: 0.10,
}

// Config controls latency range and failure behaviour.
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
	MaxLatency: 30 * time.Millisecond,
	Failure:    DefaultProfile,
}

var ProductionConfig = Config{
	MinLatency: 300 * time.Millisecond,
	MaxLatency: 1500 * time.Millisecond,
	Failure:    ProductionProfile,
}

// LLMClient simulates an external AI API. Safe for concurrent use.
type LLMClient struct {
	cfg     Config
	mu      sync.Mutex
	rng     *rand.Rand
	calls   int64 // total calls made — useful for metrics
}

// New returns an LLMClient with the given config.
func New(cfg Config) *LLMClient {
	return &LLMClient{
		cfg: cfg,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Call simulates a single LLM API call, respecting the provided context.
// Returns nil on success, or one of ErrTimeout, ErrRateLimit, ErrServerError.
func (c *LLMClient) Call(ctx context.Context, task string, articleID int) error {
	c.mu.Lock()
	spread := int64(c.cfg.MaxLatency - c.cfg.MinLatency)
	latency := c.cfg.MinLatency + time.Duration(c.rng.Int63n(spread))
	r := c.rng.Float64()
	c.calls++
	c.mu.Unlock()

	fp := c.cfg.Failure

	// Rate limit — fast failure, no latency
	if r < fp.RateLimitRate {
		fmt.Printf("  [%d] %s → 429 rate limited\n", articleID, task)
		return ErrRateLimit
	}
	r -= fp.RateLimitRate

	// Server error — fast failure
	if r < fp.ServerErrRate {
		fmt.Printf("  [%d] %s → 503 server error\n", articleID, task)
		return ErrServerError
	}
	r -= fp.ServerErrRate

	// Timeout — hangs until context deadline fires
	if r < fp.TimeoutRate {
		latency = fp.TimeoutAfter + 30*time.Second
	}

	fmt.Printf("  [%d] %s started (%v)\n", articleID, task, latency.Round(time.Millisecond))

	select {
	case <-time.After(latency):
		fmt.Printf("  [%d] %s completed\n", articleID, task)
		return nil
	case <-ctx.Done():
		fmt.Printf("  [%d] %s cancelled: %v\n", articleID, task, ctx.Err())
		return ctx.Err()
	}
}

// TotalCalls returns the total number of Call invocations since creation.
func (c *LLMClient) TotalCalls() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}
