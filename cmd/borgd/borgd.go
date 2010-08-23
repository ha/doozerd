package main

import (
	"borg"
	"borg/store"
	"flag"
	"log"
	"os"
	"strings"
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

func acquire(path string) {
	// TODO acquire a lock
	// N.B. this should block
}

func release(path string) {
	// TODO release lock
}

func run(s *store.Store, path string) {
	for {
		acquire(path)
		cmd, ok := s.Lookup(path)
		if !ok {
			return
		}

		// TODO better tokenization (e.g. quoted strings, variables)
		args := strings.Split(cmd, " ", -1)
		logger.Logf("starting %s", cmd)
		pid, err := os.ForkExec(args[0], args, nil, "/", nil)
		if err != nil {
			logger.Logf("fatal: %v", err)
			return
		}
		logger.Logf("started process %d", pid)
		w, err := os.Wait(pid, 0)
		if err != nil {
			logger.Logf("wtf: %v", err)
			return
		}
		logger.Logf("process %d exited with %v", pid, w)
		release(path)
	}
}

func main() {
	flag.Parse()
	s := store.New(logger)
	b := borg.New(*id, *listenAddr, s, logger)

	adds := make(chan store.Event)
	s.Watch("/service", store.Add, adds)
	go func() {
		for ev := range adds {
			path := ev.Path + "/" + ev.Body
			logger.Logf("oh, %s has appeared", path)
			go run(s, path)
		}
	}()

	if *attachAddr == "" {
		b.Init()
	} else {
		b.Join(*attachAddr)
	}
	b.RunForever()
}
