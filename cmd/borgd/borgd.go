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
	listenAddr *string = flag.String("l", ":804", "The address to bind to.")
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

// NOT IPv6-compatible.
func getPort(addr string) uint64 {
	parts := strings.Split(addr, ":", -1)
	port, err := strconv.Btoui64(parts[len(parts) - 1], 10)
	if err != nil {
		fmt.Printf("error getting port from %q\n", addr)
	}
	return port
}

func RecvUdp(conn net.PacketConn, ch chan paxos.Msg) {
	for {
		pkt := make([]byte, 3000) // make sure it's big enough
		n, addr, err := conn.ReadFrom(pkt)
		if err != nil {
			fmt.Println(err)
			continue
		}
		msg := parse(string(pkt[0:n]))
		msg.From = getPort(addr.String())
		ch <- msg
	}
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

func NewUdpPutter(me uint64, addrs []net.Addr, conn net.PacketConn) paxos.Putter {
	put := func(m paxos.Msg) {
		pkt := fmt.Sprintf("%d:%d:%s:%s", me, m.To, m.Cmd, m.Body)
		fmt.Printf("send udp packet %q\n", pkt)
		b := []byte(pkt)
		var to []net.Addr
		if m.To == 0 {
			to = addrs
		} else {
			to = []net.Addr{&net.UDPAddr{net.ParseIP("127.0.0.1"), int(m.To)}}
		}

		for _, addr := range to {
			n, err := conn.WriteTo(b, addr)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if n != len(b) {
				fmt.Printf("sent <%d> bytes, wanted to send <%d>\n", n, len(b))
				continue
			}
		}
	}
	return FuncPutter(put)
}

func main() {
	var basePort int
	nodes := make([]net.Addr, 5)
	flag.Parse()

	if *attachAddr != "" {
		var err os.Error
		basePort, err = strconv.Atoi((*attachAddr)[1:])
		if err != nil {
			fmt.Println(err)
			return
		}
	} else {
		var err os.Error
		basePort, err = strconv.Atoi((*listenAddr)[1:])
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	nodes[0] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 0}
	nodes[1] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 1}
	nodes[2] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 2}
	nodes[3] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 3}
	nodes[4] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 4}
	logger.Logf("attempting to attach to %v\n", nodes)

	me, err := strconv.Btoui64((*listenAddr)[1:], 10)
	if err != nil {
		fmt.Println(err)
		return
	}

	logger.Logf("attempting to listen on %s\n", *listenAddr)

	//open tcp sock

	conn, err := net.ListenPacket("udp", *listenAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	udpCh := make(chan paxos.Msg)
	go RecvUdp(conn, udpCh)
	udpPutter := NewUdpPutter(me, nodes, conn)

	manager := paxos.NewManager(uint64(len(nodes)))
	//manager.Init(FuncPutter(printMsg))
	manager.Init(udpPutter)

	go func() {
		for pkt := range udpCh {
			fmt.Printf("got udp packet: %#v\n", pkt)
			manager.Put(pkt)
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
