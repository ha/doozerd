package main

import (
	"borg/paxos"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

// Flags
var (
	listenAddr *string = flag.String("l", ":8046", "The address to bind to.")
	attachAddr *string = flag.String("a", "", "The address to bind to.")
)

// Globals
var (
	logger *log.Logger = log.New(os.Stderr, nil, "borgd: ", log.Lok)
)

const (
	mFrom = iota
	mTo
	mCmd
	mBody
	mNumParts
)

func ListenUdp(laddr string, ch chan string) os.Error {
	conn, err := net.ListenPacket("udp", laddr)
	if err != nil {
		return err
	}

	go func() {
		for {
			pkt := make([]byte, 3000) // make sure it's big enough
			n, _, err := conn.ReadFrom(pkt)
			if err != nil {
				fmt.Println(err)
				continue
			}
			ch <- string(pkt[0:n])
		}
	}()
	return nil
}

func parse(s string) paxos.Msg {
	parts := strings.Split(s, ":", mNumParts)
	if len(parts) != mNumParts {
		panic(s)
	}

	from, err := strconv.Btoui64(parts[mFrom], 10)
	if err != nil {
		panic(s)
	}

	var to uint64
	if parts[mTo] == "*" {
		to = 0
	} else {
		to, err = strconv.Btoui64(parts[mTo], 10)
		if err != nil {
			panic(err)
		}
	}

	return paxos.Msg{1, from, to, parts[mCmd], parts[mBody]}
}

type FuncPutter func (paxos.Msg)

func (f FuncPutter) Put(m paxos.Msg) {
	f(m)
}

func printMsg(m paxos.Msg) {
	fmt.Printf("should send %v\n", m)
}

func main() {
	flag.Parse()

	logger.Logf("attempting to listen on %s\n", *listenAddr)

	if *attachAddr != "" {
		logger.Logf("attempting to attach to %s\n", *attachAddr)
	}


	//open tcp sock
	udpCh := make(chan string)
	err := ListenUdp(*listenAddr, udpCh)
	if err != nil {
		fmt.Println(err)
		return
	}

	manager := paxos.NewManager()
	manager.Init(FuncPutter(printMsg))

	go func() {
		for pkt := range udpCh {
			fmt.Printf("got udp packet: %#v\n", pkt)
			manager.Put(parse(pkt))
		}
	}()

	//go func() {
	//	for m from a client:
	//		switch m.type {
	//		case 'set':
	//			go func() {
	//				v := manager.propose(encode(m))
	//				if v == m {
	//					reply 'OK'
	//				} else {
	//					reply 'fail'
	//				}
	//			}()
	//		case 'get':
	//			read from store
	//			return value
	//		}
	//}()

	for {
		seqn, v := manager.Recv()
		fmt.Printf("learned %d %#v\n", seqn, v)
		//store.Apply(manager.Recv())
	}

	// Think of borg events like inotify events.  We're interested in changes to
	// them all.  All events are sent to your instance of borg.  The filtering
	// is done once they've arrived.  Let's make keys look like directories to
	// aid this comparison.
	//
	// Example spec:
	//
	//     /<slug_id>/proc/<type>/<upid>/...
	//
	// Example interactive session:
	//
	//     $ borgd -i -a :9999
	//     >> ls /proc/123_a3c_a12b3c45/beanstalkd/12345/
	//     cmd
	//     env
	//     lock
	//     >> cat /proc/123_a3c_a12b3c45/beanstalkd/12345/*
	//     beanstalkd -l 0.0.0.0 -p 4563
	//     PORT=4563
	//     123.4.5.678:9999
	//     >>
	//
	// Example code:
	//
	//     me, err := borg.ListenAndServe(*listenAddr)
	//     if err != nil {
	//         log.Exitf("listen failed: %v", err)
	//     }
	//
	//     // Handle a specific type of key notification.
	//     // The : signals a named variable part.
	//     me.HandleFunc(
	//         "/proc/:slug/beanstalkd/:upid/lock",
	//         func (msg *borg.Message) {
	//             if msg.Value == myId {
	//                 cmd := beanstalkd ....
	//                 ... launch beanstalkd ...
	//                 me.Echo(cmd, "/proc/<slug>/beanstalkd/<upid>/cmd")
	//             }
	//         },
	//     )
}
