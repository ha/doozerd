package main


import (
	"crypto/tls"
	"doozer/peer"
	"flag"
	"fmt"
	"github.com/ha/doozer"
	"net"
	"os"
	"log"
	_ "expvar"
	"strconv"
)

const defWebPort = 8000

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
	buri        = flag.String("b", "", "boot cluster uri (tried after -a)")
	waddr       = flag.String("w", "", "web listen addr (default: see below)")
	name        = flag.String("c", "local", "The non-empty cluster name.")
	showVersion = flag.Bool("v", false, "print doozerd's version string")
	pi          = flag.Float64("pulse", 1, "how often (in seconds) to set applied key")
	fd          = flag.Float64("fill", .1, "delay (in seconds) to fill unowned seqns")
	kt          = flag.Float64("timeout", 60, "timeout (in seconds) to kick inactive nodes")
	hi          = flag.Int64("hist", 2000, "length of history/revisions to keep")
	certFile    = flag.String("tlscert", "", "TLS public certificate")
	keyFile     = flag.String("tlskey", "", "TLS private key")
)

var (
	rwsk = os.Getenv("DOOZER_RWSECRET")
	rosk = os.Getenv("DOOZER_ROSECRET")
)


func init() {
	flag.Var(&aaddrs, "a", "attach address (may be given multiple times)")
}


func Usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\nOptions:\n")
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, `
The default for -w is to use the addr from -l,
and change the port to 8000. If you give "-w false",
doozerd will not listen for for web connections.
`)
}


func main() {
	*buri = os.Getenv("DOOZER_BOOT_URI")

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

	if *certFile != "" || *keyFile != "" {
		tsock = tlsWrap(tsock, *certFile, *keyFile)
	}

	uaddr, err := net.ResolveUDPAddr("udp", *laddr)
	if err != nil {
		panic(err)
	}

	usock, err := net.ListenUDP("udp", uaddr)
	if err != nil {
		panic(err)
	}

	var wsock net.Listener
	if *waddr == "" {
		wa, err := net.ResolveTCPAddr("tcp", *laddr)
		if err != nil {
			panic(err)
		}
		wa.Port = defWebPort
		*waddr = wa.String()
	}
	if b, err := strconv.Atob(*waddr); err != nil && !b {
		wsock, err = net.Listen("tcp", *waddr)
		if err != nil {
			panic(err)
		}
	}

	id := randId()
	var cl *doozer.Conn
	switch {
	case len(aaddrs) > 0 && *buri != "":
		cl = attach(*name, aaddrs)
		if cl == nil {
			cl = boot(*name, id, *laddr, *buri)
		}
	case len(aaddrs) > 0:
		cl = attach(*name, aaddrs)
		if cl == nil {
			panic("failed to attach")
		}
	case *buri != "":
		cl = boot(*name, id, *laddr, *buri)
	}

	peer.Main(*name, id, *buri, rwsk, rosk, cl, usock, tsock, wsock, ns(*pi), ns(*fd), ns(*kt), *hi)
	panic("main exit")
}

func ns(x float64) int64 {
	return int64(x * 1e9)
}


func tlsWrap(l net.Listener, cfile, kfile string) net.Listener {
	if cfile == "" || kfile == "" {
		panic("need both cert file and key file")
	}

	cert, err := tls.LoadX509KeyPair(cfile, kfile)
	if err != nil {
		panic(err)
	}

	tc := new(tls.Config)
	tc.Certificates = append(tc.Certificates, cert)
	return tls.NewListener(l, tc)
}
