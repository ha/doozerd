package junta

import (
	"os"
	"net"
	"log"
	
	"junta/util"
	"junta/proto"
)

type conn struct {
	net.Conn
}

type server struct {
	net.Listener
	logger *log.Logger
}

func Serve(l net.Listener) os.Error {
	for {
		rw, e := l.Accept()
		if e != nil {
			//s.logger.Logf("%s: %s", s.Listener, err)
			return e
		}
		c := &conn{rw}
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
			rlogger.Log("len(parts) == 0")
			rlogger.Log("before error")
			pc.SendError(rid, proto.InvalidCommand)
			rlogger.Log("after error")
			continue
		}

		switch parts[0] {
		default:
			rlogger.Logf("unknown command <%s>", parts[0])
			pc.SendError(rid, proto.InvalidCommand)
		case "set":
			//go set(c, rid, parts[1:])
			rlogger.Logf("set %q=%q", parts[1], parts[2])
			pc.SendResponse(rid, "OK")
		}
		rlogger.Log("bottom")
	}
}

func ListenAndServe(addr string) os.Error {
	logger := util.NewLogger("server %s", addr)

	logger.Logf("binding to %s", addr)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Log(err)
		return err
	}
	logger.Logf("listening on %s", addr)

	err = Serve(l)
	l.Close()
	return err
}
