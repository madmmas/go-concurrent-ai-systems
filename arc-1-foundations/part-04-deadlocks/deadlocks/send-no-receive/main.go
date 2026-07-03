// Deadlock Demo 1: Send with no receiver.
//
// This is the most common deadlock beginners hit.
// An unbuffered channel send BLOCKS until someone receives.
// If nobody ever receives, the goroutine waits forever.
//
// Run this:
//
//	go run ./deadlocks/send-no-receive
//
// You will see:
//
//	fatal error: all goroutines are asleep - deadlock!
//
// The Go runtime detects that every goroutine is blocked
// and nobody can make progress. It kills the program.
package main

import "fmt"

func main() {
	ch := make(chan string) // unbuffered — send blocks until someone receives

	// Nobody is receiving from ch.
	// This send blocks forever.
	// The Go runtime detects the deadlock and panics.
	ch <- "article summary"

	// This line is never reached.
	fmt.Println("sent!")
}
