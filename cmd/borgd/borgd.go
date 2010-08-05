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
			case "set":
				if arity < 2 {
					req.RespondErrf("%d for 2 arguments", arity)
					break
				}
				values[string(req.Parts[1])] = req.Parts[2]
				req.RespondOk()
			case "get":
				if arity < 1 {
					req.RespondErrf("%d for 1 arguments", arity)
					break
				}

				got, ok := values[string(req.Parts[1])]
				switch ok {
					case true:  req.Respond(got)
					case false: req.RespondNil()
				}
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
