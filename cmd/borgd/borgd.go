package main

import (
	"borg"
	"borg/store"
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
	s := store.New(logger)
	b := borg.New(*id, *listenAddr, s, logger)

	//adds := make(chan store.Event)
	adds := s.Watch("/service", store.Add)
	go func() {
		for ev := range adds {
			logger.Logf("oh, %s/%s has appeared", ev.Path, ev.Body)
		}
	}()

	if *attachAddr == "" {
		b.Init()
	} else {
		b.Join(*attachAddr)
	}
	b.RunForever()
}
