// Command broken demos the Part 2 failure modes:
//
//	go run ./cmd/broken -mode=no-waitgroup
//	go run ./cmd/broken -mode=loop-capture
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-1-foundations/part-02-goroutines/internal/simulator"
)

func main() {
	n    := flag.Int("articles", 5, "number of articles")
	mode := flag.String("mode", "no-waitgroup", "no-waitgroup or loop-capture")
	flag.Parse()

	llm := simulator.New(simulator.FastConfig)
	articles := pipeline.GenerateArticles(*n)

	start := time.Now()
	switch *mode {
	case "no-waitgroup":
		fmt.Println("BUG: launching goroutines with no WaitGroup")
		fmt.Println("Expect finish in µs — work never completes before return.")
		fmt.Println("─────────────────────────────────────────────────────────")
		pipeline.NewBroken(llm).ProcessAll(articles)
		// Give orphaned goroutines a moment so some logs may appear.
		time.Sleep(50 * time.Millisecond)

	case "loop-capture":
		fmt.Println("BUG: loop variable captured by reference")
		fmt.Println("Expect many workers to process the same (last) article.")
		fmt.Println("─────────────────────────────────────────────────────────")
		pipeline.NewLoopCapture(llm).ProcessAll(articles)

	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", *mode)
		os.Exit(1)
	}

	fmt.Printf("\nFinished in %v\n", time.Since(start))
}
