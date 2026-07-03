// Package simulator provides a fake LLM client with realistic failure modes.
//
// Part 6 introduces the first failure behaviour: timeouts.
// A real LLM API call can hang for many seconds before the provider
// gives up. Without a deadline, one slow call blocks a worker forever.
//
// FailureProfile controls how often each failure mode fires.
// Start with all rates at zero (DefaultProfile) to reproduce Part 5
// behaviour, then increase TimeoutRate to see the problem Parts 6 solves.
package simulator

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// FailureProfile controls the probability of each failure mode.
// Rates are in [0.0, 1.0] — 0.0 means never, 1.0 means always.
type FailureProfile struct {
	TimeoutRate float64       // probability call exceeds TimeoutAfter
	TimeoutAfter time.Duration // how long before a timeout fires
}

// DefaultProfile — no failures, matches Arc 1 behaviour exactly.
var DefaultProfile = FailureProfile{
	TimeoutRate:  0.0,
	TimeoutAfter: 5 * time.Second,
}

// UnreliableProfile — 20% of calls time out. Realistic for a busy LLM provider.
var UnreliableProfile = FailureProfile{
	TimeoutRate:  0.20,
	TimeoutAfter: 2 * time.Second,
}

// Config controls latency range.
type Config struct {
	MinLatency time.Duration
	MaxLatency time.Duration
	Failure    FailureProfile
}

var DefaultConfig = Config{
	MinLatency: 500 * time.Millisecond,
	MaxLatency: 1500 * time.Millisecond,
	Failure:    DefaultProfile,
}

var FastConfig = Config{
	MinLatency: 10 * time.Millisecond,
	MaxLatency: 50 * time.Millisecond,
	Failure:    DefaultProfile,
}

// UnreliableConfig — realistic production conditions: variable latency + timeouts.
var UnreliableConfig = Config{
	MinLatency: 500 * time.Millisecond,
	MaxLatency: 3000 * time.Millisecond,
	Failure:    UnreliableProfile,
}

// LLMClient simulates an external AI API. Safe for concurrent use.
type LLMClient struct {
	cfg Config
	mu  sync.Mutex
	rng *rand.Rand
}

// New returns an LLMClient with the given config.
func New(cfg Config) *LLMClient {
	return &LLMClient{
		cfg: cfg,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// ErrTimeout is returned when a simulated call exceeds its deadline.
var ErrTimeout = fmt.Errorf("llm: call timed out")

// Call simulates a single LLM API call, respecting the provided context.
//
// If ctx carries a deadline and the simulated latency exceeds it, Call
// returns ErrTimeout — matching the behaviour of a real HTTP client that
// respects context cancellation.
//
// This is the key addition in Part 6: every call now takes a context.
// Callers that don't set a deadline get the same behaviour as Part 5.
// Callers that set a timeout get protection against slow providers.
func (c *LLMClient) Call(ctx context.Context, task string, articleID int) error {
	c.mu.Lock()
	spread := int64(c.cfg.MaxLatency - c.cfg.MinLatency)
	latency := c.cfg.MinLatency + time.Duration(c.rng.Int63n(spread))
	willTimeout := c.rng.Float64() < c.cfg.Failure.TimeoutRate
	c.mu.Unlock()

	if willTimeout {
		// Simulate a call that hangs until the context deadline fires.
		// We sleep for longer than any reasonable timeout would allow.
		latency = c.cfg.Failure.TimeoutAfter + 10*time.Second
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
