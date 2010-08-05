package main

import (
	"borg/proto"
	"bufio"
	"fmt"
	"net"
)

var values = make(map[string][]byte)

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:9999")
	if err != nil {
		panic(err)
	}

	ch := make(chan *proto.Request)
	go func() {
		//PAXOS!
		for req := range ch {
			fmt.Printf("Err:%v | Parts:%v\n", req.Err, req.Parts)
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		go proto.Scan(bufio.NewReader(conn), ch)
	}

}
