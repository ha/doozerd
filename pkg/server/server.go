package server

import (
	jnet "doozer/net"
	"doozer/paxos"
	"doozer/proto"
	"doozer/store"
	"doozer/util"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const packetSize = 3000

const lease = 3e9 // ns == 3s

var ErrBadPrefix = os.NewError("bad prefix in path")

var responded = os.NewError("already responded")

type conn struct {
	*proto.Conn
	c   net.Conn
	s   *Server
	cal bool
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
		c := &conn{proto.NewConn(rw), rw, s, ok}
		go c.serve()
	}

	panic("unreachable")
}

func (sv *Server) leader() string {
	parts, cas := sv.St.Get("/doozer/leader")
	if cas == store.Dir && cas == store.Missing {
		return ""
	}
	return parts[0]
}

func (sv *Server) addrFor(id string) string {
	parts, cas := sv.St.Get("/doozer/members/" + id)
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

// Repeatedly propose nop values until a successful read from `done`.
func (sv *Server) AdvanceUntil(done chan int) {
	for _, ok := <-done; !ok; _, ok = <-done {
		sv.Mg.Propose(store.Nop)
	}
}

func (c *conn) redirect(rid uint) {
	leader := c.s.leader()
	addr := c.s.addrFor(leader)
	if addr == "" {
		c.SendError(rid, "unknown address for leader")
	} else {
		c.SendRedirect(rid, addr)
	}
}

func get(c *conn, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqGet)
	shortPath, err := c.s.checkPath(r.Path)
	if err != nil {
		return nil, err
	}

	v, cas := c.s.St.Get(shortPath)
	return proto.ResGet{v, cas}, nil
}

func sget(c *conn, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqGet)
	shortPath, err := c.s.checkPath(r.Path)
	if err != nil {
		return "", err
	}

	body, err := store.GetString(c.s.St.SyncPath(shortPath), shortPath), nil
	if err != nil {
		return nil, err
	}

	return body, nil
}

func set(c *conn, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqSet)

	shortPath, err := c.s.checkPath(r.Path)
	if err != nil {
		return nil, err
	}

	seqn, _, err := paxos.Set(c.s.Mg, shortPath, r.Body, r.Cas)
	if err != nil {
		return nil, err
	}
	return seqn, nil
}

func del(c *conn, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqDel)

	shortPath, err := c.s.checkPath(r.Path)
	if err != nil {
		return 0, err
	}

	seqn, err := paxos.Del(c.s.Mg, shortPath, r.Cas)
	if err != nil {
		return nil, err
	}

	return seqn, nil
}

func nop(c *conn, data interface{}) (interface{}, os.Error) {
	c.s.Mg.Propose(store.Nop)
	return nil, nil
}

func join(c *conn, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqJoin)
	key := "/doozer/members/" + r.Who
	seqn, _, err := paxos.Set(c.s.Mg, key, r.Addr, store.Missing)
	if err != nil {
		return nil, err
	}

	done := make(chan int)
	go c.s.AdvanceUntil(done)
	c.s.St.Sync(seqn + uint64(c.s.Mg.Alpha()))
	close(done)
	seqn, snap := c.s.St.Snapshot()
	return proto.ResJoin{seqn, snap}, nil
}

func checkin(c *conn, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqCheckin)
	t := time.Nanoseconds() + lease
	_, cas, err := paxos.Set(c.s.Mg, "/session/"+r.Sid, strconv.Itoa64(t), r.Cas)
	if err != nil {
		return nil, err
	}
	return proto.ResCheckin{t, cas}, nil
}

func indirect(x interface{}) interface{} {
	return reflect.Indirect(reflect.NewValue(x)).Interface()
}

type handler func(*conn, interface{}) (interface{}, os.Error)

type op struct {
	p interface{}
	f handler

	redirect bool
}

var ops = map[string]op{
	"get":{p:new(*proto.ReqGet), f:get},
	"sget":{p:new(*proto.ReqGet), f:sget},
	"set":{p:new(*proto.ReqSet), f:set, redirect:true},
	"del":{p:new(*proto.ReqDel), f:del, redirect:true},
	"nop":{p:new(*[]interface{}), f:nop, redirect:true},
	"join":{p:new(*proto.ReqJoin), f:join, redirect:true},
	"checkin":{p:new(*proto.ReqCheckin), f:checkin, redirect:true},
}

func (c *conn) handle(rid uint, f handler, data interface{}) {
	res, err := f(c, data)
	if err == responded {
		return
	}

	if err != nil {
		c.SendError(rid, err.String())
	} else {
		c.SendResponse(rid, proto.Last, res)
	}
}

func (c *conn) serve() {
	logger := util.NewLogger("%v", c.c.RemoteAddr())
	logger.Println("accepted connection")
	for {
		rid, verb, data, err := c.ReadRequest()
		if err != nil {
			if err == os.EOF {
				logger.Println("connection closed by peer")
			} else {
				logger.Println(err)
			}
			return
		}

		rlogger := util.NewLogger("%v - req [%d]", c.c.RemoteAddr(), rid)

		if o, ok := ops[verb]; ok {
			rlogger.Printf("%s %v", verb, data)

			err := proto.Fit(data, o.p)
			if err != nil {
				c.SendError(rid, err.String())
				continue
			}

			if o.redirect && !c.cal {
				c.redirect(rid)
				continue
			}

			go c.handle(rid, o.f, indirect(o.p))
			continue
		}

		rlogger.Printf("unknown command <%s>", verb)
		c.SendError(rid, proto.InvalidCommand+" "+verb)
	}
}
