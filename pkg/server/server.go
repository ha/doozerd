package server

import (
	jnet "junta/net"
	"junta/paxos"
	"junta/proto"
	"junta/store"
	"junta/util"
	"net"
	"os"
	"strconv"
	"strings"
)

const packetSize = 3000

var ErrBadPrefix = os.NewError("bad prefix in path")

type conn struct {
	net.Conn
	s *Server
}

type Manager interface {
	paxos.Proposer
	PutFrom(string, paxos.Msg)
	Alpha() int
}

type Server struct {
	Addr         string
	St           *store.Store
	Mg           Manager
	Self, Prefix string
}

func (sv *Server) ListenAndServeUdp(outs chan paxos.Packet) os.Error {
	logger := util.NewLogger("udp server %s", sv.Addr)

	logger.Println("binding")
	u, err := net.ListenPacket("udp", sv.Addr)
	if err != nil {
		logger.Println(err)
		return err
	}
	defer u.Close()
	logger.Println("listening")

	err = sv.ServeUdp(u, outs)
	if err != nil {
		logger.Printf("%s: %s", u, err)
	}
	return err
}

func (sv *Server) ServeUdp(u jnet.Conn, outs chan paxos.Packet) os.Error {
	r := jnet.Ackify(u, outs)

	for p := range r {
		sv.Mg.PutFrom(p.Addr, p.Msg)
	}

	panic("unreachable")
}

var clg = util.NewLogger("cal")

func (s *Server) Serve(l net.Listener, cal chan int) os.Error {
	var ok bool
	for {
		if !ok {
			_, ok = <-cal
		}
		clg.Println(ok)


		rw, e := l.Accept()
		if e != nil {
			return e
		}
		c := &conn{rw, s}
		go c.serve(ok)
	}

	panic("unreachable")
}

func (sv *Server) leader() string {
	parts, cas := sv.St.Get("/junta/leader")
	if cas == store.Dir && cas == store.Missing {
		return ""
	}
	return parts[0]
}

func (sv *Server) addrFor(id string) string {
	parts, cas := sv.St.Get("/junta/members/" + id)
	if cas == store.Dir && cas == store.Missing {
		return ""
	}
	return parts[0]
}

// Checks that path begins with the proper prefix and returns the short path
// without the prefix.
func (sv *Server) checkPath(path string) (string, os.Error) {
	logger := util.NewLogger("checkPath")
	if !strings.HasPrefix(path, sv.Prefix+"/") {
		logger.Printf("prefix %q not in %q", sv.Prefix+"/", path)
		return "", ErrBadPrefix
	}
	return path[len(sv.Prefix):], nil
}

func (sv *Server) Set(path, body, cas string) (uint64, string, os.Error) {
	shortPath, err := sv.checkPath(path)
	if err != nil {
		return 0, "", err
	}

	return paxos.Set(sv.Mg, shortPath, body, cas)
}

func (sv *Server) Nop() {
	sv.Mg.Propose(store.Nop)
}

func (sv *Server) Del(path, cas string) (uint64, os.Error) {
	shortPath, err := sv.checkPath(path)
	if err != nil {
		return 0, err
	}

	return paxos.Del(sv.Mg, shortPath, cas)
}

func (sv *Server) Get(path string) (v []string, cas string, err os.Error) {
	var shortPath string
	shortPath, err = sv.checkPath(path)
	if err != nil {
		return
	}

	v, cas = sv.St.Get(shortPath)
	return
}

func (sv *Server) Sget(path string) (body string, err os.Error) {
	shortPath, err := sv.checkPath(path)
	if err != nil {
		return "", err
	}

	return store.GetString(sv.St.SyncPath(shortPath), shortPath), nil
}

// Repeatedly propose nop values until a successful read from `done`.
func (sv *Server) AdvanceUntil(done chan int) {
	for _, ok := <-done; !ok; _, ok = <-done {
		sv.Nop()
	}
}

func (c *conn) serve(cal bool) {
	pc := proto.NewConn(c)
	logger := util.NewLogger("%v", c.RemoteAddr())
	logger.Println("accepted connection")
	for {
		rid, parts, err := pc.ReadRequest()
		if err != nil {
			if err == os.EOF {
				logger.Println("connection closed by peer")
			} else {
				logger.Println(err)
			}
			return
		}

		rlogger := util.NewLogger("%v - req [%d]", c.RemoteAddr(), rid)
		rlogger.Printf("received <%v>", parts)

		if len(parts) == 0 {
			rlogger.Println("zero parts supplied")
			pc.SendError(rid, proto.InvalidCommand+": no command")
			continue
		}

		switch parts[0] {
		default:
			rlogger.Printf("unknown command <%s>", parts[0])
			pc.SendError(rid, proto.InvalidCommand+" "+parts[0])
		case "set":
			if len(parts) != 4 {
				rlogger.Printf("invalid set command: %#v", parts)
				pc.SendError(rid, "wrong number of parts")
				break
			}

			leader := c.s.leader()
			if !cal {
				addr := c.s.addrFor(leader)
				if addr == "" {
					rlogger.Printf("unknown address for leader: %s", leader)
					pc.SendError(rid, "unknown address for leader")
					break
				}

				rlogger.Printf("redirect to %s", addr)
				pc.SendRedirect(rid, addr)
				break
			}

			rlogger.Printf("set %q=%q (cas %q)", parts[1], parts[2], parts[3])
			seqn, _, err := c.s.Set(parts[1], parts[2], parts[3])
			if err != nil {
				rlogger.Printf("bad: %s", err)
				pc.SendError(rid, err.String())
			} else {
				rlogger.Printf("good")
				pc.SendResponse(rid, []interface{}{strconv.Uitoa64(seqn)})
			}
		case "del":
			if len(parts) != 3 {
				rlogger.Printf("invalid del command: %v", parts)
				pc.SendError(rid, "wrong number of parts")
				break
			}

			leader := c.s.leader()
			if !cal {
				rlogger.Printf("redirect to %s", leader)
				pc.SendRedirect(rid, leader)
				break
			}

			rlogger.Printf("del %q (cas %q)", parts[1], parts[2])
			_, err := c.s.Del(parts[1], parts[2])
			if err != nil {
				rlogger.Printf("bad: %s", err)
				pc.SendError(rid, err.String())
			} else {
				rlogger.Printf("good")
				pc.SendResponse(rid, []interface{}{"true"})
			}
		case "get":
			if len(parts) != 2 {
				rlogger.Printf("invalid get command: %v", parts)
				pc.SendError(rid, "wrong number of parts")
				break
			}
			rlogger.Printf("get %q", parts[1])
			v, cas, err := c.s.Get(parts[1])
			if err != nil {
				rlogger.Printf("bad: %s", err)
				pc.SendError(rid, err.String())
			} else {
				rlogger.Println("good get cas", cas)
				pc.SendResponse(rid, []interface{}{v, cas})
			}
		case "sget":
			if len(parts) != 2 {
				rlogger.Printf("invalid sget command: %v", parts)
				pc.SendError(rid, "wrong number of parts")
				break
			}
			rlogger.Printf("sget %q", parts[1])
			body, err := c.s.Sget(parts[1])
			if err != nil {
				rlogger.Printf("bad: %s", err)
				pc.SendError(rid, err.String())
			} else {
				rlogger.Printf("good %q", body)
				pc.SendResponse(rid, []interface{}{body})
			}
		case "nop":
			if len(parts) != 1 {
				rlogger.Printf("invalid nop command: %v", parts)
				pc.SendError(rid, "wrong number of parts")
				break
			}

			leader := c.s.leader()
			if !cal {
				rlogger.Printf("redirect to %s", leader)
				pc.SendRedirect(rid, leader)
				break
			}

			rlogger.Println("nop")
			c.s.Nop()
			rlogger.Printf("good")
			pc.SendResponse(rid, []interface{}{"true"})
		case "join":
			// join abc123 1.2.3.4:999
			if len(parts) != 3 {
				rlogger.Printf("invalid join command: %v", parts)
				pc.SendError(rid, "wrong number of parts")
				break
			}

			leader := c.s.leader()
			if !cal {
				rlogger.Printf("redirect to %s", leader)
				pc.SendRedirect(rid, leader)
				break
			}

			who, addr := parts[1], parts[2]
			rlogger.Printf("membership requested for %s at %s", who, addr)

			key := c.s.Prefix + "/junta/members/" + who

			seqn, _, err := c.s.Set(key, addr, store.Missing)
			if err != nil {
				rlogger.Printf("bad: %s", err)
				pc.SendError(rid, err.String())
			} else {
				rlogger.Printf("good")
				done := make(chan int)
				go c.s.AdvanceUntil(done)
				c.s.St.Sync(seqn + uint64(c.s.Mg.Alpha()))
				close(done)
				seqn, snap := c.s.St.Snapshot()
				pc.SendResponse(rid, []interface{}{strconv.Uitoa64(seqn), snap})
			}
		}
	}
}
