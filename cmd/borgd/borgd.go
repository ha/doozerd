package main

import (
	"borg"
	"fmt"
	"net"
)

var values = make(map[string][]byte)

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:9999")
	if err != nil {
		panic(err)
	}

	bus := make(chan *borg.Request)
	go func() {
		//PAXOS!
		var arity int
		for req := range bus {
			if req.Err != nil {
				fmt.Printf("Err:%v | Parts:%v\n", req.Err, req.Parts)
				continue
			}

			arity = len(req.Parts) - 1

			switch string(req.Parts[0]) {
			default:
				fmt.Printf("OTHER! %v\n", req)
			case "set":
				fmt.Printf("SET!\n")
				if arity < 2 {
					fmt.Printf("-ERR: %d for 2 arguments\n", arity)
					break
				}
				values[string(req.Parts[1])] = req.Parts[2]
			case "get":
				fmt.Printf("GET!\n")
				if arity < 1 {
					fmt.Printf("-ERR: %d for 1 arguments\n", arity)
					break
				}
				got := values[string(req.Parts[1])]
				fmt.Printf("got: %v\n", got)
			}
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		go borg.Relay(conn, bus)
	}

}
