// Correct version: all three deadlocks fixed.
//
// Fix 1 (send-no-receive): always ensure a goroutine is receiving
//   before sending on an unbuffered channel.
//
// Fix 2 (circular-wait): break the cycle by using goroutines
//   so sends and receives are not mutually blocking.
//
// Fix 3 (forgotten-close): always close(ch) after the last send.
//   Use defer to guarantee it runs even on early return or panic.
//
// Run this:
//
//	go run ./deadlocks/correct
package main

import (
	"fmt"
	"sync"
)

func main() {
	demonstrateSendReceive()
	demonstrateNoCircularWait()
	demonstrateForgottenClose()
}

// Fix 1: receiver exists before sender sends.
func demonstrateSendReceive() {
	ch := make(chan string)

	go func() {
		ch <- "article summary" // sender in goroutine
	}()

	msg := <-ch // receiver in main — both sides exist, no deadlock
	fmt.Println("Fix 1 — received:", msg)
}

// Fix 2: use goroutines to break the circular dependency.
func demonstrateNoCircularWait() {
	chA := make(chan string)
	chB := make(chan string)

	go func() {
		chA <- "result from A"
	}()
	go func() {
		chB <- "result from B"
	}()

	// Receive from both — no circular wait because sends are non-blocking.
	msgA := <-chA
	msgB := <-chB
	fmt.Printf("Fix 2 — A: %s, B: %s\n", msgA, msgB)
}

// Fix 3: always close the channel after the last send.
func demonstrateForgottenClose() {
	articles := make(chan int)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(articles) // guaranteed to run — range will exit cleanly
		for i := 1; i <= 3; i++ {
			articles <- i
		}
	}()

	go func() {
		wg.Wait() // not needed here but shows the pattern in larger systems
	}()

	for id := range articles {
		fmt.Printf("Fix 3 — processing article %d\n", id)
	}
	fmt.Println("Fix 3 — done")
}
