package main

import (
	"flag"
	"log"
	"os"
)

// Flags
var (
	id         *string = flag.String("i", "", "Node id to use.")
	listenAddr *string = flag.String("l", ":8040", "The address to bind to.")
	attachAddr *string = flag.String("a", "", "The address to bind to.")
)

// Globals
var (
	logger *log.Logger = log.New(os.Stderr, nil, "juntad: ", log.Lok)
)

func main() {
	flag.Parse()
}
