// Command news-processor demonstrates graceful shutdown via context cancellation.
//
// Normal run — processes all articles:
//
//	go run ./cmd/news-processor -articles=10 -workers=5
//
// Simulate a SIGTERM arriving mid-pipeline (cancel after 2s):
//
//	go run ./cmd/news-processor -articles=10 -workers=3 -cancel-after=2s
//
// The ShutdownReport shows exactly what completed, what was in-flight,
// and what never started — so you know the exact state when it stopped.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-09-cancellation/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-09-cancellation/internal/simulator"
)

func main() {
	n           := flag.Int("articles", 10, "number of articles")
	w           := flag.Int("workers", 5, "number of workers")
	timeout     := flag.Duration("timeout", 4*time.Second, "per-article timeout")
	cancelAfter := flag.Duration("cancel-after", 0, "simulate cancellation after duration (0 = no cancel)")
	flag.Parse()

	if *n <= 0 || *w <= 0 {
		fmt.Fprintln(os.Stderr, "articles and workers must be > 0")
		os.Exit(1)
	}

	// Base context: cancelled by OS signal (SIGTERM, SIGINT) or -cancel-after flag.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	if *cancelAfter > 0 {
		fmt.Printf("⏱  Will cancel pipeline after %v\n", *cancelAfter)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *cancelAfter)
		defer cancel()
	}

	llm := simulator.New(simulator.DefaultConfig)
	pool := pipeline.New(llm, *w, *timeout)
	articles := pipeline.GenerateArticles(*n)

	fmt.Printf("Pipeline: %d articles, %d workers, %v per-article timeout\n",
		*n, *w, *timeout)
	fmt.Println("Press Ctrl+C to trigger graceful shutdown at any point.")
	fmt.Println("─────────────────────────────────────────────────────────")

	_, report := pool.ProcessAll(ctx, articles)

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Succeeded : %d\n", report.Succeeded)
	fmt.Printf("Failed    : %d (timeout/error)\n", report.Failed)
	fmt.Printf("Cancelled : %d (in-flight when shutdown arrived)\n", report.Cancelled)
	fmt.Printf("Queued    : %d (never started)\n", report.Queued)
	fmt.Printf("Duration  : %v\n", report.Duration)
	fmt.Println("═════════════════════════════════════════════════════════")

	total := report.Succeeded + report.Failed + report.Cancelled + report.Queued
	fmt.Printf("\nTotal accounted for: %d / %d\n", total, *n)
}
