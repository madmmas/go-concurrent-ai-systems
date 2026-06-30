// Package broken demonstrates the data race introduced in Part 2.
//
// Run with the race detector to see it clearly:
//
//	go run -race ./broken
//
// You will see output like:
//
//	WARNING: DATA RACE
//	Write at 0x00c0001a4010 by goroutine 8:
//	  main.processArticle()
//	Previous write at 0x00c0001a4010 by goroutine 7:
//	  main.processArticle()
//
// Sometimes the result count will be correct (10). Often it will be wrong
// (7, 8, 9). The inconsistency is the hallmark of a race condition —
// behaviour that depends on goroutine scheduling, which is non-deterministic.
//
// The fix is in internal/pipeline/processor.go.
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type article struct {
	ID int
}

type aiResult struct {
	ArticleID int
	Summary   string
}

// results is the shared slice — the race target.
var results []aiResult

func main() {
	rand.Seed(time.Now().UnixNano())

	articles := generateArticles(10)

	var wg sync.WaitGroup
	for _, a := range articles {
		wg.Add(1)
		go func(art article) {
			defer wg.Done()
			processArticle(art)
		}(a)
	}
	wg.Wait()

	fmt.Printf("\nExpected: %d results\n", len(articles))
	fmt.Printf("Actual  : %d results\n", len(results))
	fmt.Println("Run several times — the actual count will vary.")
	fmt.Println("Run with -race to see the detector fire.")
}

func processArticle(a article) {
	// Simulate variable LLM latency so goroutines genuinely interleave.
	time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)

	result := aiResult{
		ArticleID: a.ID,
		Summary:   "AI-generated summary",
	}

	// DATA RACE: multiple goroutines append to the same slice simultaneously.
	// append() may resize the slice, reallocate memory, and update length/capacity.
	// If two goroutines do this at the same moment, writes overwrite each other.
	results = append(results, result)

	fmt.Printf("Stored article %d (results len: %d)\n", a.ID, len(results))
}

func generateArticles(n int) []article {
	arts := make([]article, n)
	for i := range arts {
		arts[i] = article{ID: i + 1}
	}
	return arts
}
