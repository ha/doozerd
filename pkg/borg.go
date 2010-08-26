package borg

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
//     exe
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
//     me, err := borg.ListenAndServe(listenAddr)
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

import (
	"borg/paxos"
	"borg/proto"
	"borg/store"
	"borg/util"
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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

func recvUdp(conn net.PacketConn, ch chan paxos.Message) {
	for {
		pkt := make([]byte, 3000) // make sure it's big enough
		n, _, err := conn.ReadFrom(pkt)
		if err != nil {
			fmt.Println(err)
			continue
		}
		msg := paxos.NewMessage(string(pkt[0:n]))
		ch <- msg
	}
}

type funcPutter func (paxos.Message)

func (f funcPutter) Put(m paxos.Message) {
	f(m)
}

func newUdpPutter(me uint64, addrs []net.Addr, conn net.PacketConn) paxos.Putter {
	put := func(m paxos.Message) {
		pkt := fmt.Sprintf("%d:%d:0:%s:%s", m.Seqn(), me, m.Cmd(), m.Body())
		b := []byte(pkt)

		for _, addr := range addrs {
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
	return funcPutter(put)
}

type Node struct {
	id string
	listenAddr string
	logger *log.Logger
	nodes []net.Addr
	store *store.Store
	manager *paxos.Manager
}

func New(id string, listenAddr string, s *store.Store, logger *log.Logger) *Node {
	if id == "" {
		b := make([]byte, 8)
		util.RandBytes(b)
		id = fmt.Sprintf("%x", b)
	}
	return &Node{
		listenAddr:listenAddr,
		logger:logger,
		store:s,
		id:id,
	}
}

func (n *Node) Init() {
	var basePort int
	var err os.Error
	basePort, err = strconv.Atoi((n.listenAddr)[1:])
	n.nodes = make([]net.Addr, 5)
	if err != nil {
		fmt.Println(err)
		return
	}
	n.nodes[0] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 0}
	n.nodes[1] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 1}
	n.nodes[2] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 2}
	n.nodes[3] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 3}
	n.nodes[4] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 4}

	nodeKey := "/node/" + n.id
	mut, err := store.EncodeSet(nodeKey, n.listenAddr)
	if err != nil {
		panic(err)
	}
	n.store.Apply(1, mut)
	n.logger.Logf("registered %s at %s\n", n.id, n.listenAddr)
}

// TODO this function should take only an address and get all necessary info
// from the other existing nodes.
func (n *Node) Join(master string) {
	parts := strings.Split(master, "=", 2)
	if len(parts) < 2 {
		panic(fmt.Sprintf("bad master address: %s", master))
	}
	mid, addr := parts[0], parts[1]


	var basePort int
	var err os.Error
	n.nodes = make([]net.Addr, 5)
	basePort, err = strconv.Atoi((addr)[1:])
	if err != nil {
		fmt.Println(err)
		return
	}
	n.nodes[0] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 0}
	n.nodes[1] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 1}
	n.nodes[2] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 2}
	n.nodes[3] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 3}
	n.nodes[4] = &net.UDPAddr{net.ParseIP("127.0.0.1"), basePort + 4}
	n.logger.Logf("attempting to attach to %v\n", n.nodes)

	n.logger.Logf("TODO: get a snapshot")
	// TODO remove all this fake stuff and talk to the other nodes
	// BEGIN FAKE STUFF
	nodeKey := "/node/" + mid
	mut, err := store.EncodeSet(nodeKey, addr)
	if err != nil {
		panic(err)
	}
	n.store.Apply(1, mut)
	// END OF FAKE STUFF
}

func (n *Node) server(conn net.Conn) {
	br := bufio.NewReader(conn)
	for {
		parts, err := proto.Decode(br)
		switch err {
		case os.EOF:
			return
		case nil:
			// nothing
		default:
			n.logger.Log(err)
			continue
		}

		if len(parts) == 0 {
			continue
		}

		n.logger.Log("got", parts)
		switch parts[0] {
		case "set":
			mutation, err := store.EncodeSet(parts[1], parts[2])
			if err != nil {
				io.WriteString(conn, fmt.Sprintf("-ERR: %s", err))
			}
			v := n.manager.Propose(mutation)
			if v == mutation {
				proto.Encodef(conn, "OK")
			} else {
				io.WriteString(conn, "-ERR: failed")
			}
		case "get":
			body, ok := n.store.Lookup(parts[1])
			if ok {
				proto.Encodef(conn, "%s", body)
			} else {
				io.WriteString(conn, "-ERR: missing")
			}
		default:
			io.WriteString(conn, "-ERR: unknown command")
		}
	}
}

func (n *Node) accept(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			n.logger.Log(err)
			continue
		}
		go n.server(c)
	}
}

func (n *Node) RunForever() {
	me, err := strconv.Btoui64((n.listenAddr)[1:], 10)
	if err != nil {
		fmt.Println(err)
		return
	}

	n.logger.Logf("attempting to listen on %s\n", n.listenAddr)

	tcpListener, err := net.Listen("tcp", n.listenAddr)
	if err != nil {
		n.logger.Log(err)
		return
	}
	go n.accept(tcpListener)

	udpConn, err := net.ListenPacket("udp", n.listenAddr)
	if err != nil {
		fmt.Println(err)
		return
	}

	udpCh := make(chan paxos.Message)
	go recvUdp(udpConn, udpCh)
	udpPutter := newUdpPutter(me, n.nodes, udpConn)

	n.manager = paxos.NewManager(2, n.id, []string{n.id}, udpPutter, n.logger)

	go func() {
		for pkt := range udpCh {
			n.manager.Put(pkt)
		}
	}()

	for {
		n.store.Apply(n.manager.Recv())
	}
}
