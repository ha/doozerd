package doozer

import (
	"fmt"
	"rand"
	"testing"
	"time"
)

// TODO make sure all these goroutines are cleaned up nicely

func randN() int32 {
	return rand.Int31n(252) + 1
}

func randAddr() string {
	port := rand.Int31n(63000) + 2000
	return fmt.Sprintf("127.%d.%d.%d:%d", randN(), randN(), randN(), port)
}

func TestFoo(t *testing.T) {
	a0, w := randAddr(), randAddr()
	fmt.Println("web", w)
	go Main("a", a0, "", w)
	time.Sleep(1e9)
	go Main("a", randAddr(), a0, "")
	go Main("a", randAddr(), a0, "")
	time.Sleep(3e9)
}
