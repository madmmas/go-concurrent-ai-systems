// Command news-processor is the runnable entry point for Part 1 of the
// "Production-Grade Concurrent AI Systems in Go" series.
//
// It demonstrates the sequential baseline: ten articles, each requiring
// three simulated LLM calls, processed one by one. Run it, note the total
// time, then think about what the number looks like at 10 000 articles.
//
// Usage:
//
//	go run ./cmd/news-processor
//	go run ./cmd/news-processor -articles=50
package main

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-01-sequential/internal/pipeline"
	"github.com/moinuddin/go-concurrent-ai-systems/arc-1-foundations/part-01-sequential/internal/simulator"
)

func main() {
	n := flag.Int("articles", 10, "number of articles to process")
	flag.Parse()

	if *n <= 0 {
		fmt.Fprintln(os.Stderr, "error: -articles must be a positive integer")
		os.Exit(1)
	}

	llm := simulator.New(simulator.DefaultConfig)
	proc := pipeline.New(llm)
	articles := pipeline.GenerateArticles(*n)

	fmt.Printf("Starting sequential pipeline — %d articles, 3 AI tasks each\n", *n)
	fmt.Println("─────────────────────────────────────────────────────────")

	results, duration := proc.ProcessAll(articles)

	fmt.Println("\n═════════════════════════════════════════════════════════")
	fmt.Printf("Processed : %d articles\n", len(results))
	fmt.Printf("Total time: %v\n", duration.Round(1000000)) // round to ms
	fmt.Println("═════════════════════════════════════════════════════════")

	printScalingProjection(*n, duration)
}

// printScalingProjection shows how total time would grow linearly if we
// kept the sequential design and simply increased the article count.
// This is the table from the blog post — rendered directly from the real
// measured duration so the numbers are always honest.
func printScalingProjection(measured int, duration interface{ Seconds() float64 }) {
	perArticle := duration.Seconds() / float64(measured)

	counts := []int{10, 100, 1_000, 10_000}

	fmt.Println("\nScaling projection (sequential design):")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintln(w, "  Articles\tEstimated time")
	fmt.Fprintln(w, "  ────────\t──────────────")
	for _, c := range counts {
		secs := perArticle * float64(c)
		fmt.Fprintf(w, "  %d\t%s\n", c, formatDuration(secs))
	}
	_ = w.Flush()
	fmt.Println()
}

func formatDuration(secs float64) string {
	switch {
	case secs < 60:
		return fmt.Sprintf("%.0fs", secs)
	case secs < 3600:
		return fmt.Sprintf("%.1f min", secs/60)
	default:
		return fmt.Sprintf("%.1f hours", secs/3600)
	}
}
