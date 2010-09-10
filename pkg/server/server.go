package server

import (
	"os"
	"net"
	
	"junta/util"
	"junta/paxos"
	"junta/proto"
	"junta/store"
	"strconv"
)

type conn struct {
	net.Conn
	s *Server
}

type Server struct {
	Addr string
	St *store.Store
	Mg *paxos.Manager
}

func (sv *Server) ListenAndServe() os.Error {
	logger := util.NewLogger("server %s", sv.Addr)

	logger.Log("binding")
	l, err := net.Listen("tcp", sv.Addr)
	if err != nil {
		logger.Log(err)
		return err
	}
	defer l.Close()
	logger.Log("listening")

	err = sv.Serve(l)
	if err != nil {
		logger.Logf("%s: %s", l, err)
	}
	return err
}

func (sv *Server) ListenAndServeUdp(outs chan paxos.Msg) os.Error {
	logger := util.NewLogger("udp server %s", sv.Addr)

	logger.Log("binding")
	u, err := net.ListenPacket("udp", sv.Addr)
	if err != nil {
		logger.Log(err)
		return err
	}
	defer u.Close()
	logger.Log("listening")

	err = sv.ServeUdp(u, outs)
	if err != nil {
		logger.Logf("%s: %s", u, err)
	}
	return err
}

func (sv *Server) ServeUdp(u net.PacketConn, outs chan paxos.Msg) os.Error {
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
			sv.Mg.PutFrom(addr, msg)
		}
	}()

	logger.Log("sending messages...")
	for msg := range outs {
		logger.Logf("sending %v", msg)
		for _, addr := range sv.Mg.AddrsFor(msg) {
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

func (s *Server) Serve(l net.Listener) os.Error {
	for {
		rw, e := l.Accept()
		if e != nil {
			return e
		}
		c := &conn{rw, s}
		go c.serve()
	}

	panic("not reached")
}

func (sv *Server) Set(path, body, cas string) (uint64, os.Error) {
	mut, err := store.EncodeSet(path, body, cas)
	if err != nil {
		return 0, err
	}

	seqn, v, err := sv.Mg.Propose(mut)
	if err != nil {
		return 0, err
	}

	// We failed, but only because of a competing proposal. The client should
	// retry.
	if v != mut {
		return 0, os.EAGAIN
	}

	return seqn, nil
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
			if len(parts) != 4 {
				rlogger.Logf("invalid set command: %#v", parts)
				pc.SendError(rid, "wrong number of parts")
				break
			}
			rlogger.Logf("set %q=%q (cas %q)", parts[1], parts[2], parts[3])
			err := os.EAGAIN
			for err == os.EAGAIN {
				_, err = c.s.Set(parts[1], parts[2], parts[3])
			}
			if err != nil {
				rlogger.Logf("bad: %s", err)
				pc.SendError(rid, err.String())
			} else {
				rlogger.Logf("good")
				pc.SendResponse(rid, "true")
			}
		case "join":
			// join abc123 1.2.3.4:999
			if len(parts) != 3 {
				rlogger.Logf("invalid join command: %v", parts)
				pc.SendError(rid, "wrong number of parts")
				break
			}

			who, addr := parts[1], parts[2]
			rlogger.Logf("membership requested for %s at %s", who, addr)

			key := "/j/junta/members/" + who

			var seqn uint64
			err := os.EAGAIN
			for err == os.EAGAIN {
				seqn, err = c.s.Set(key, addr, store.Missing)
			}
			if err != nil {
				rlogger.Logf("bad: %s", err)
				pc.SendError(rid, err.String())
			} else {
				rlogger.Logf("good")
				c.s.St.Sync(seqn + uint64(c.s.Mg.Alpha))
				seqn, snap := c.s.St.Snapshot()
				pc.SendResponse(rid, strconv.Uitoa64(seqn), snap)
			}
		}
	}
}
