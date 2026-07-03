// Package simulator — same as Part 6, copied forward unchanged.
// Part 7 doesn't change failure behaviour — it changes how the pipeline
// responds to external signals (OS signals, manual cancellation).
package simulator

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type FailureProfile struct {
	TimeoutRate  float64
	TimeoutAfter time.Duration
}

var DefaultProfile = FailureProfile{TimeoutRate: 0.0, TimeoutAfter: 5 * time.Second}
var UnreliableProfile = FailureProfile{TimeoutRate: 0.20, TimeoutAfter: 2 * time.Second}

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

var UnreliableConfig = Config{
	MinLatency: 500 * time.Millisecond,
	MaxLatency: 3000 * time.Millisecond,
	Failure:    UnreliableProfile,
}

type LLMClient struct {
	cfg Config
	mu  sync.Mutex
	rng *rand.Rand
}

func New(cfg Config) *LLMClient {
	return &LLMClient{cfg: cfg, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

func (c *LLMClient) Call(ctx context.Context, task string, articleID int) error {
	c.mu.Lock()
	spread := int64(c.cfg.MaxLatency - c.cfg.MinLatency)
	latency := c.cfg.MinLatency + time.Duration(c.rng.Int63n(spread))
	willTimeout := c.rng.Float64() < c.cfg.Failure.TimeoutRate
	c.mu.Unlock()

	if willTimeout {
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
