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

func NewLogger(format string, a ... interface{}) *log.Logger {
	prefix := fmt.Sprintf(format, a)

	if prefix == "" {
		panic("always give a prefix!")
	}

	return log.New(
		os.Stderr, nil,
		"juntad: " + prefix + " ",
		log.Lok | log.Lshortfile,
	)
}

func main() {
	flag.Parse()

	logger := NewLogger("main")

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
		go serveConn(conn)
	}
}

func serveConn(conn net.Conn) {
	c := proto.NewConn(conn)
	_, parts, err := c.ReadRequest()
	logger := NewLogger("%v", conn.RemoteAddr())
	logger.Logf("accepted connection")
	if err != nil {
		logger.Log(err)
		return
	}
	logger.Logf("recvd req <%v>", parts)
}
