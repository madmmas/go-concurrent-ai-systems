// Package simulator provides a fake LLM client that mimics the latency
// characteristics of real AI API calls without requiring network access or
// API keys. This lets readers run every example locally and reason about
// timing without external dependencies.
package simulator

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Config controls the latency behaviour of the simulated LLM.
type Config struct {
	// MinLatency is the floor for a simulated API call.
	MinLatency time.Duration
	// MaxLatency is the ceiling. Actual latency is chosen uniformly at random
	// in [MinLatency, MaxLatency].
	MaxLatency time.Duration
}

// DefaultConfig reflects realistic LLM API latency in a production
// environment: 500ms on the low end, 1500ms on the high end.
var DefaultConfig = Config{
	MinLatency: 500 * time.Millisecond,
	MaxLatency: 1500 * time.Millisecond,
}

// FastConfig is useful for unit tests that verify correctness rather than
// timing; it keeps the suite fast without removing the sleep entirely.
var FastConfig = Config{
	MinLatency: 10 * time.Millisecond,
	MaxLatency: 50 * time.Millisecond,
}

// LLMClient simulates an external AI API.
// Multiple goroutines may call the same client concurrently; the mutex
// protects the shared RNG without holding the lock during time.Sleep.
type LLMClient struct {
	cfg Config
	mu  sync.Mutex
	rng *rand.Rand
}

// New returns an LLMClient using the provided config and a
// deterministic random source seeded from the current time.
func New(cfg Config) *LLMClient {
	return &LLMClient{
		cfg: cfg,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Call simulates a single LLM API call for the named task on articleID.
// It blocks for a random duration within the configured latency window,
// then returns, mimicking a successful API response.
//
// Output format matches the blog post exactly:
//
//	[1] Summarization started (1.1s)
//	[1] Summarization completed
func (c *LLMClient) Call(task string, articleID int) {
	c.mu.Lock()
	spread := int64(c.cfg.MaxLatency - c.cfg.MinLatency)
	latency := c.cfg.MinLatency + time.Duration(c.rng.Int63n(spread))
	c.mu.Unlock()

	fmt.Printf("  [%d] %s started (%v)\n", articleID, task, latency.Round(time.Millisecond))
	time.Sleep(latency)
	fmt.Printf("  [%d] %s completed\n", articleID, task)
}

// Latency returns a single random duration within the configured window.
// Useful for benchmarks and tests that need a latency sample without
// triggering the print side-effects of Call.
func (c *LLMClient) Latency() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	spread := int64(c.cfg.MaxLatency - c.cfg.MinLatency)
	return c.cfg.MinLatency + time.Duration(c.rng.Int63n(spread))
}
