package paxos

import(
	"borg/proto"
	"bufio"
	"fmt"
	"net"
	"os"
	//"strconv"
	//"strings"
)

type instance struct {
	id uint64
}

type conn struct {
	net.Conn
}

type Paxos struct {
	nextInstanceId uint64
	ch chan *proto.Request
	conns map[string]*conn
	laddr string
}

var (
	EmptyRequest = os.NewError("empty request")
	WrongArity = os.NewError("wrong arity")
)

func New(addr string) *Paxos {
	return &Paxos{
		ch:make(chan *proto.Request),
		laddr:addr,
	}
}

func (p *Paxos) RunForever(errs chan os.Error) {
	go p.pump(errs)

	listener, err := net.Listen("tcp", p.laddr)
	if err != nil {
		errs <- err
		return
	}

	for {
		c, err := listener.Accept()
		if err != nil {
			errs <- err
			continue
		}

		conn := &conn{
			Conn:c,
		}

		p.conns[c.RemoteAddr().String()] = conn
		go proto.Scan(bufio.NewReader(c), p.ch)
	}
}

func (p *Paxos) pump(errs chan os.Error) {
	for {
		req := <-p.ch
		if req.Err != nil {
			errs <- req.Err
			continue
		}

		if len(req.Parts) == 0 {
			errs <- EmptyRequest
			continue
		}

		switch string(req.Parts[0]) {
		default:
			panic("aaah")
		case "set":
			if len(req.Parts) != 4 {
				errs <- WrongArity
				continue
			}

			p.set(req.Parts[1], req.Parts[2], req.Parts[3])
		}
	}
}

func (p *Paxos) set(k, v, cas []byte) {
	fmt.Printf("set %q %q (%s)\n", k, v, cas)
}

