package server

import (
	"os"
	"net"
	
	"junta/util"
	"junta/paxos"
	"junta/proto"
	"junta/store"
)

const window = 50

type conn struct {
	net.Conn
	s *server
}

type server struct {
	net.Listener
	st *store.Store
	mg *paxos.Manager
}

func Serve(l net.Listener, st *store.Store, mg *paxos.Manager) os.Error {
	return (&server{l, st, mg}).Serve()
}

func ServeUdp(u net.PacketConn, mg *paxos.Manager, outs chan paxos.Msg) os.Error {
	logger := util.NewLogger("udp server %s", u.LocalAddr())
	go func() {
		logger.Log("reading messages...")
		for {
			// TODO pull out this magic number into a const
			msg, addr, err := paxos.ReadMsg(u, 3000)
			if err != nil {
				logger.Log(err)
				continue
			}
			logger.Logf("read %v from %s", msg, addr)
			mg.PutFrom(addr, msg)
		}
	}()

	logger.Log("sending messages...")
	for msg := range outs {
		logger.Logf("sending %v", msg)
		for _, addr := range mg.AddrsFor(msg) {
			logger.Logf("sending to %s", addr)
			udpAddr, err := net.ResolveUDPAddr(addr)
			if err != nil {
				logger.Log(err)
				continue
			}
			_, err = u.WriteTo(msg.WireBytes(), udpAddr)
			if err != nil {
				logger.Log(err)
				continue
			}
		}
	}

	panic("not reached")
}

func (s *server) Serve() os.Error {
	for {
		rw, e := s.Accept()
		if e != nil {
			return e
		}
		c := &conn{rw, s}
		go c.serve()
	}

	panic("not reached")
}

func (c *conn) serve() {
	pc := proto.NewConn(c)
	logger := util.NewLogger("%v", c.RemoteAddr())
	logger.Log("accepted connection")
	for {
		rid, parts, err := pc.ReadRequest()
		if err != nil {
			if err == os.EOF {
				logger.Log("connection closed by peer")
			} else {
				logger.Log(err)
			}
			return
		}

		rlogger := util.NewLogger("%v - req [%d]", c.RemoteAddr(), rid)
		rlogger.Logf("received <%v>", parts)

		if len(parts) == 0 {
			rlogger.Log("zero parts supplied")
			pc.SendError(rid, proto.InvalidCommand)
			continue
		}

		switch parts[0] {
		default:
			rlogger.Logf("unknown command <%s>", parts[0])
			pc.SendError(rid, proto.InvalidCommand)
		case "set":
			//go set(c, rid, parts[1:])
			rlogger.Logf("set %q=%q (cas %q)", parts[1], parts[2], parts[3])
			mut, err := store.EncodeSet(parts[1], parts[2], parts[3])
			if err != nil {
				rlogger.Log(err)
				pc.SendError(rid, err.String())
			} else {
				rlogger.Logf("propose %q", mut)
				v := c.s.mg.Propose(mut)
				if v == mut {
					rlogger.Logf("good")
					pc.SendResponse(rid, "true")
				} else {
					rlogger.Logf("bad")
					pc.SendError(rid, "false")
				}
			}
		}
	}
}

func ListenAndServe(addr string, st *store.Store, mg *paxos.Manager) os.Error {
	logger := util.NewLogger("server %s", addr)

	logger.Log("binding")
	l, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Log(err)
		return err
	}
	defer l.Close()
	logger.Log("listening")

	err = Serve(l, st, mg)
	if err != nil {
		logger.Logf("%s: %s", l, err)
	}
	return err
}

func ListenAndServeUdp(addr string, mg *paxos.Manager, outs chan paxos.Msg) os.Error {
	logger := util.NewLogger("udp server %s", addr)

	logger.Log("binding")
	u, err := net.ListenPacket("udp", addr)
	if err != nil {
		logger.Log(err)
		return err
	}
	defer u.Close()
	logger.Log("listening")

	err = ServeUdp(u, mg, outs)
	if err != nil {
		logger.Logf("%s: %s", u, err)
	}
	return err
}
