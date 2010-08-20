package main

import (
	"borg"
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
	logger *log.Logger = log.New(os.Stderr, nil, "borgd: ", log.Lok)
)

func main() {
	flag.Parse()
	b := borg.New(*id, *listenAddr, logger)
	if *attachAddr == "" {
		b.Init()
	} else {
		b.Join(*attachAddr)
	}
	b.RunForever()
}
