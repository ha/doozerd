package junta

import (
	"os"
	"net"
	
	"junta/util"
	"junta/proto"
)

func ListenAndServe(addr string) os.Error {
	logger := util.NewLogger("main")

	logger.Logf("binding to %s", addr)
	lisn, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Log("unable to listen on %s: %s", addr, err)
		os.Exit(1)
	}

	logger.Logf("listening on %s", addr)

	for {
		conn, err := lisn.Accept()
		if err != nil {
			logger.Log("unable to accept on %s: %s", addr, err)
			continue
		}
		go serveConn(conn)
	}

	return nil
}

func serveConn(conn net.Conn) {
	c := proto.NewConn(conn)
	logger := util.NewLogger("%v", conn.RemoteAddr())
	logger.Logf("accepted connection")
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
			rlogger.Logf("len(parts) == 0")
			rlogger.Logf("before error")
			c.SendError(rid, proto.InvalidCommand)
			rlogger.Logf("after error")
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
		rlogger.Logf("bottom")
	}
}

