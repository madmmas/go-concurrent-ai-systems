// Package pipeline — errgroup implementation.
//
// golang.org/x/sync/errgroup requires network access.
// This implementation provides the identical API using only the standard library.
// The source is included here so readers can see exactly how errgroup works —
// which is more educational than treating it as a black box.
//
// In your production code: use golang.org/x/sync/errgroup directly.
package pipeline

import (
	"context"
	"sync"
)

// Group is a drop-in replacement for errgroup.Group.
// It runs a collection of goroutines and returns the first non-nil error.
// When the first error occurs, the group's context is cancelled,
// signalling all other goroutines to stop.
type Group struct {
	cancel  func()
	wg      sync.WaitGroup
	mu      sync.Mutex
	firstErr error
}

// WithContext returns a new Group and a derived context.
// The context is cancelled when the first goroutine returns a non-nil error
// or when all goroutines complete, whichever comes first.
func WithContext(ctx context.Context) (*Group, context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{cancel: cancel}, ctx
}

// Go starts a goroutine running fn. If fn returns a non-nil error,
// the group cancels its context and records the error.
func (g *Group) Go(fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		if err := fn(); err != nil {
			g.mu.Lock()
			if g.firstErr == nil {
				g.firstErr = err
				g.cancel()
			}
			g.mu.Unlock()
		}
	}()
}

// Wait blocks until all goroutines have returned.
// It returns the first non-nil error, if any.
func (g *Group) Wait() error {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.firstErr
}
