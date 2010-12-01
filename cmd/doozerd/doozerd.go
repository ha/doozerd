package main

import (
	"doozer"
	"flag"
	"fmt"
	"net"
	"os"
)

// Flags
var (
	listenAddr  = flag.String("l", "127.0.0.1:8046", "The address to bind to.")
	attachAddr  = flag.String("a", "", "The address of another node to attach to.")
	webAddr     = flag.String("w", ":8080", "Serve web requests on this address.")
	clusterName = flag.String("c", "local", "The non-empty cluster name.")
)

func Usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] <cluster-name>\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
}
func main() {
	flag.Parse()
	flag.Usage = Usage

	if *listenAddr == "" {
		fmt.Fprintln(os.Stderr, "require a listen address")
		flag.Usage()
		os.Exit(1)
	}

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		panic(err)
	}


	doozer.Main(*clusterName, *attachAddr, *webAddr, listener)
}
