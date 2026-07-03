// Deadlock Demo 2: Circular wait between two goroutines.
//
// Goroutine A sends on chA and waits to receive from chB.
// Goroutine B sends on chB and waits to receive from chA.
// Both are blocked waiting for the other — circular deadlock.
//
// This mirrors a real production pattern: two pipeline stages
// each waiting for the other to produce before consuming.
//
// Run this:
//
//	go run ./deadlocks/circular-wait
//
// You will see:
//
//	fatal error: all goroutines are asleep - deadlock!
package main

import "fmt"

func main() {
	chA := make(chan string)
	chB := make(chan string)

	// Goroutine A: sends on chA, then waits to receive from chB.
	go func() {
		chA <- "result from A"     // blocks until main receives from chA
		msg := <-chB               // then waits for B — but B is also waiting
		fmt.Println("A got:", msg)
	}()

	// Goroutine B: sends on chB, then waits to receive from chA.
	go func() {
		chB <- "result from B"     // blocks until main receives from chB
		msg := <-chA               // then waits for A — but A is also waiting
		fmt.Println("B got:", msg)
	}()

	// Main receives neither — it just waits forever.
	// Both goroutines are blocked. Deadlock.
	select {}
}
