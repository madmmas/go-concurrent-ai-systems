// Package simulator provides a fake LLM client that mimics realistic
// AI API latency without requiring network access or API keys.
package simulator

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Config struct {
	MinLatency time.Duration
	MaxLatency time.Duration
}

var DefaultConfig = Config{
	MinLatency: 500 * time.Millisecond,
	MaxLatency: 1500 * time.Millisecond,
}

var FastConfig = Config{
	MinLatency: 10 * time.Millisecond,
	MaxLatency: 50 * time.Millisecond,
}

// LLMClient simulates an external AI API. Safe for concurrent use.
type LLMClient struct {
	cfg Config
	mu  sync.Mutex
	rng *rand.Rand
}

func New(cfg Config) *LLMClient {
	return &LLMClient{
		cfg: cfg,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Call simulates a single LLM API call, blocking for a random latency.
func (c *LLMClient) Call(task string, articleID int) {
	c.mu.Lock()
	spread := int64(c.cfg.MaxLatency - c.cfg.MinLatency)
	latency := c.cfg.MinLatency + time.Duration(c.rng.Int63n(spread))
	c.mu.Unlock()

	fmt.Printf("  [%d] %s started (%v)\n", articleID, task, latency.Round(time.Millisecond))
	time.Sleep(latency)
	fmt.Printf("  [%d] %s completed\n", articleID, task)
}
