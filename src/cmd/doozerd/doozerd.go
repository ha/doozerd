package main


import (
	"doozer/peer"
	"flag"
	"fmt"
	"github.com/ha/doozer"
	"net"
	"os"
	"log"
	_ "expvar"
	_ "http/pprof"
)

type strings []string


func (a *strings) Set(s string) bool {
	*a = append(*a, s)
	return true
}


func (a *strings) String() string {
	return fmt.Sprint(*a)
}


var (
	laddr       = flag.String("l", "127.0.0.1:8046", "The address to bind to.")
	aaddrs      = strings{}
	baddr       = flag.String("b", "", "boot cluster address (tried after -a)")
	webAddr     = flag.String("w", ":8080", "Serve web requests on this address.")
	name        = flag.String("c", "local", "The non-empty cluster name.")
	showVersion = flag.Bool("v", false, "print doozerd's version string")
	pi          = flag.Float64("pulse", 1, "how often (in seconds) to set applied key")
	fd          = flag.Float64("fill", .1, "delay (in seconds) to fill unowned seqns")
	kt          = flag.Float64("timeout", 60, "timeout (in seconds) to kick inactive nodes")
)


func init() {
	flag.Var(&aaddrs, "a", "attach address (may be given multiple times)")
}


func Usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
}


func main() {
	flag.Usage = Usage
	flag.Parse()

	if *showVersion {
		fmt.Println("doozerd", peer.Version)
		return
	}

	if *laddr == "" {
		fmt.Fprintln(os.Stderr, "require a listen address")
		flag.Usage()
		os.Exit(1)
	}

	log.SetPrefix("DOOZER ")
	log.SetFlags(log.Ldate | log.Lmicroseconds)

	tsock, err := net.Listen("tcp", *laddr)
	if err != nil {
		panic(err)
	}

	usock, err := net.ListenPacket("udp", *laddr)
	if err != nil {
		panic(err)
	}

	var wsock net.Listener
	if *webAddr != "" {
		wsock, err = net.Listen("tcp", *webAddr)
		if err != nil {
			panic(err)
		}
	}

	id := randId()
	var cl *doozer.Conn
	switch {
	case len(aaddrs) > 0 && *baddr != "":
		cl = attach(*name, aaddrs)
		if cl == nil {
			cl = boot(*name, id, *laddr, *baddr)
		}
	case len(aaddrs) > 0:
		cl = attach(*name, aaddrs)
		if cl == nil {
			panic("failed to attach")
		}
	case *baddr != "":
		cl = boot(*name, id, *laddr, *baddr)
	}

	peer.Main(*name, id, *baddr, cl, usock, tsock, wsock, ns(*pi), ns(*fd), ns(*kt))
	panic("main exit")
}

func ns(x float64) int64 {
	return int64(x * 1e9)
}
