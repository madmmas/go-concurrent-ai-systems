// Package pipeline implements a circuit breaker for Part 15.
//
// Part 14 limits call rate to avoid 429s.
// Part 15 adds a circuit breaker that detects when a provider is unhealthy
// and stops calling it entirely — failing fast instead of waiting for timeouts.
//
// States:
//   Closed   — normal operation, calls go through
//   Open     — provider unhealthy, calls rejected immediately (fail fast)
//   HalfOpen — test probe: one call allowed, if it succeeds → Closed, else → Open
//
// Transitions:
//   Closed → Open:     consecutive failures exceed threshold
//   Open → HalfOpen:  cooldown period elapses
//   HalfOpen → Closed: probe call succeeds
//   HalfOpen → Open:  probe call fails
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-15-circuit-breaker/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-15-circuit-breaker/internal/simulator"
)

// ErrCircuitOpen is returned when the circuit breaker is open.
var ErrCircuitOpen = errors.New("circuit breaker: open — provider unhealthy")

type state int

const (
	stateClosed   state = iota
	stateOpen
	stateHalfOpen
)

func (s state) String() string {
	switch s {
	case stateClosed:   return "CLOSED"
	case stateOpen:     return "OPEN"
	case stateHalfOpen: return "HALF-OPEN"
	default:            return "UNKNOWN"
	}
}

// CircuitBreaker implements the three-state circuit breaker pattern.
type CircuitBreaker struct {
	mu            sync.Mutex
	state         state
	failures      int
	threshold     int           // failures before opening
	cooldown      time.Duration // time to wait before half-open
	openedAt      time.Time
	probeInFlight bool // half-open: only one probe call at a time
}

// NewCircuitBreaker returns a CircuitBreaker.
// threshold: consecutive failures before opening
// cooldown: time to wait in open state before probing
func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:     stateClosed,
		threshold: threshold,
		cooldown:  cooldown,
	}
}

// Allow returns nil if the call should proceed, ErrCircuitOpen if not.
func (cb *CircuitBreaker) Allow() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case stateClosed:
		return nil

	case stateOpen:
		if time.Since(cb.openedAt) >= cb.cooldown {
			fmt.Println("[circuit-breaker] cooldown elapsed → HALF-OPEN")
			cb.state = stateHalfOpen
			cb.probeInFlight = true
			return nil // allow the probe call
		}
		return ErrCircuitOpen

	case stateHalfOpen:
		if cb.probeInFlight {
			return ErrCircuitOpen // one probe already in flight
		}
		cb.probeInFlight = true
		return nil // allow one probe call at a time
	}
	return nil
}

// RecordSuccess records a successful call.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == stateHalfOpen {
		fmt.Println("[circuit-breaker] probe succeeded → CLOSED")
	}
	cb.state = stateClosed
	cb.failures = 0
	cb.probeInFlight = false
}

// RecordFailure records a failed call and may open the circuit.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures++
	cb.probeInFlight = false
	if cb.state == stateHalfOpen || cb.failures >= cb.threshold {
		cb.state = stateOpen
		cb.openedAt = time.Now()
		fmt.Printf("[circuit-breaker] %d failures → OPEN (cooldown %v)\n",
			cb.failures, cb.cooldown)
	}
}

// State returns the current circuit breaker state (for monitoring).
func (cb *CircuitBreaker) State() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state.String()
}

// CBPool wraps a worker pool with a circuit breaker.
type CBPool struct {
	llm     *simulator.LLMClient
	Workers int
	Timeout time.Duration
	cb      *CircuitBreaker
}

// New returns a CBPool with a circuit breaker.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration, cb *CircuitBreaker) *CBPool {
	if workers <= 0 {
		panic("CBPool: Workers must be > 0")
	}
	return &CBPool{llm: llm, Workers: workers, Timeout: timeout, cb: cb}
}

// ProcessAll processes articles, fail-fast when the circuit is open.
func (p *CBPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	jobs      := make(chan model.Article, len(articles))
	resultsCh := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for article := range jobs {
				select {
				case <-ctx.Done():
					resultsCh <- model.AIResult{ArticleID: article.ID, Err: ctx.Err()}
				default:
					articleCtx, cancel := context.WithTimeout(ctx, p.Timeout)
					resultsCh <- p.processArticle(articleCtx, article)
					cancel()
				}
			}
		}()
	}

	for _, a := range articles {
		jobs <- a
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	var results []model.AIResult
	for r := range resultsCh {
		results = append(results, r)
	}
	return results, time.Since(start)
}

func (p *CBPool) processArticle(ctx context.Context, article model.Article) model.AIResult {
	result := model.AIResult{ArticleID: article.ID}

	// Check circuit breaker BEFORE calling LLM
	if err := p.cb.Allow(); err != nil {
		fmt.Printf("[article %d] circuit OPEN — rejected\n", article.ID)
		result.Err = err
		return result
	}

	if err := p.llm.Call(ctx, "Summarisation", article.ID); err != nil {
		p.cb.RecordFailure()
		result.Err = err
		return result
	}
	p.cb.RecordSuccess()
	result.Summary = "AI-generated summary"

	if err := p.llm.Call(ctx, "Sentiment Analysis", article.ID); err != nil {
		p.cb.RecordFailure()
		result.Err = err
		return result
	}
	p.cb.RecordSuccess()
	result.Sentiment = "Positive"
	result.Keywords = []string{"AI", "Go", "CircuitBreaker"}
	fmt.Printf("[article %d] processed (circuit: %s)\n", article.ID, p.cb.State())
	return result
}

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{ID: i + 1, Title: fmt.Sprintf("Breaking News %d", i+1)}
	}
	return articles
}
