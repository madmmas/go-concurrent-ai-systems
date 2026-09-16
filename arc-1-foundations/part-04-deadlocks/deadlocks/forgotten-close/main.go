// Deadlock Demo 3: Forgotten channel close.
//
// A range loop over a channel exits when the channel is closed.
// If the channel is never closed, range blocks forever
// after consuming all available values.
//
// This is an extremely common production bug in pipeline code:
// the producer finishes but forgets to close the channel,
// leaving the consumer blocked forever, looking like a hang.
//
// Run this:
//
//	go run ./deadlocks/forgotten-close
//
// You will see three article IDs printed, then:
//
//	fatal error: all goroutines are asleep - deadlock!
package main

import "fmt"

func main() {
	articles := make(chan int)

	go func() {
		for i := 1; i <= 3; i++ {
			articles <- i
		}
		// BUG: forgot to call close(articles)
		// The range below will block forever after consuming 1, 2, 3.
	}()

	// range exits when articles is closed.
	// Since it's never closed, this blocks after the 3rd article.
	for id := range articles {
		fmt.Printf("processing article %d\n", id)
	}

	fmt.Println("done") // never reached
}
