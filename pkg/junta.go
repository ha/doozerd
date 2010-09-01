package junta

import (
	"os"
	"net"
	"log"
	
	"junta/util"
	"junta/proto"
)

type server struct {
	net.Listener
	logger *log.Logger
}

func (s server) acceptAndServe() {
	for {
		conn, err := s.Accept()
		if err != nil {
			s.logger.Logf("%s: %s", s.Listener, err)
			continue
		}
		go s.serve(conn)
	}
}

func (server) serve(conn net.Conn) {
	c := proto.NewConn(conn)
	logger := util.NewLogger("%v", conn.RemoteAddr())
	logger.Log("accepted connection")
	for {
		rid, parts, err := c.ReadRequest()
		if err != nil {
			if err == os.EOF {
				logger.Log("connection closed by peer")
			} else {
				logger.Log(err)
			}
			return
		}

		rlogger := util.NewLogger("%v - req [%d]", conn.RemoteAddr(), rid)
		rlogger.Logf("received <%v>", parts)

		if len(parts) == 0 {
			rlogger.Log("len(parts) == 0")
			rlogger.Log("before error")
			c.SendError(rid, proto.InvalidCommand)
			rlogger.Log("after error")
			continue
		}

		switch parts[0] {
		default:
			rlogger.Logf("unknown command <%s>", parts[0])
			c.SendError(rid, proto.InvalidCommand)
		case "set":
			//go set(c, rid, parts[1:])
			rlogger.Logf("set %q=%q", parts[1], parts[2])
			c.SendResponse(rid, "OK")
		}
		rlogger.Log("bottom")
	}
}

func ListenAndServe(addr string) os.Error {
	logger := util.NewLogger("server %s", addr)

	logger.Logf("binding to %s", addr)
	lisn, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Log(err)
		return err
	}

	logger.Logf("listening on %s", addr)

	server := &server{lisn, logger}
	server.acceptAndServe()

	return nil
}
