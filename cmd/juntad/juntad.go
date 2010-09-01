package main

import (
	"flag"
	"log"
	"os"
	"net"
	"fmt"

	"junta/proto"
)

// Flags
var (
	id         *string = flag.String("i", "", "Node id to use.")
	listenAddr *string = flag.String("l", ":8040", "The address to bind to.")
	attachAddr *string = flag.String("a", "", "The address to bind to.")
)

// Globals
var (
	logger *log.Logger = log.New(
		os.Stderr, nil,
		"juntad: ",
		log.Lok | log.Lshortfile,
	)
)

func main() {
	flag.Parse()

	logger.Logf("binding to %s", *listenAddr)
	lisn, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		logger.Log("unable to listen on %s: %s", *listenAddr, err)
		os.Exit(1)
	}
	logger.Logf("listening on %s", *listenAddr)

	for {
		conn, err := lisn.Accept()
		if err != nil {
			logger.Log("unable to accept on %s: %s", *listenAddr, err)
			continue
		}

		go func() {
			c := proto.NewConn(conn)
			_, parts, err := c.ReadRequest()
			if err != nil {
				logger.Log(err)
				return
			}
			fmt.Println(parts)
		}()
	}
}
