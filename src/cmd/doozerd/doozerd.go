package main


import (
	"doozer"
	"doozer/util"
	"flag"
	"fmt"
	"net"
	"os"
)


var (
	listenAddr  = flag.String("l", "127.0.0.1:8046", "The address to bind to.")
	attachAddr  = flag.String("a", "", "The address of another node to attach to.")
	webAddr     = flag.String("w", ":8080", "Serve web requests on this address.")
	clusterName = flag.String("c", "local", "The non-empty cluster name.")
	showVersion = flag.Bool("v", false, "print doozerd's version string")
	pi = flag.Float64("pulse", 1, "how often (in seconds) to set applied key")
	fd = flag.Float64("fill", 1.5, "delay (in seconds) to fill unowned seqns")
	kt = flag.Float64("timeout", 60, "timeout (in seconds) to kick inactive nodes")
)


func Usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
}


func main() {
	util.LogWriter = os.Stderr
	flag.Usage = Usage
	flag.Parse()

	if *showVersion {
		fmt.Println("doozerd", doozer.Version)
		return
	}

	if *listenAddr == "" {
		fmt.Fprintln(os.Stderr, "require a listen address")
		flag.Usage()
		os.Exit(1)
	}

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		panic(err)
	}

	conn, err := net.ListenPacket("udp", *listenAddr)
	if err != nil {
		panic(err)
	}

	var wl net.Listener
	if *webAddr != "" {
		wl, err = net.Listen("tcp", *webAddr)
		if err != nil {
			panic(err)
		}
	}

	doozer.Main(*clusterName, *attachAddr, conn, listener, wl, ns(*pi), ns(*fd), ns(*kt))
}

func ns(x float64) int64 {
	return int64(x * 1e9)
}
