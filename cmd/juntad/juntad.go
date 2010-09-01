package main

import (
	"flag"
	"junta"
)

// Flags
var (
	listenAddr *string = flag.String("l", ":8040", "The address to bind to.")
	attachAddr *string = flag.String("a", "", "The address to bind to.")
)

func main() {
	flag.Parse()
	junta.ListenAndServe(*listenAddr)
}
