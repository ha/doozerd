package server

import (
	jnet "junta/net"
	"junta/paxos"
	"junta/proto"
	"junta/store"
	"junta/util"
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

func (sv *Server) Checkin(id, cas string) (t int64, ncas string, err os.Error) {
	t = time.Nanoseconds() + lease
	_, ncas, err = paxos.Set(sv.Mg, "/session/"+id, strconv.Itoa64(t), cas)
	return
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

func (c *conn) redirect(rid uint) {
	leader := c.s.leader()
	addr := c.s.addrFor(leader)
	if addr == "" {
		c.SendError(rid, "unknown address for leader")
	} else {
		c.SendRedirect(rid, addr)
	}
}

func get(s *Server, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqGet)

	v, cas, err := s.Get(r.Path)
	if err != nil {
		return nil, err
	}
	return []interface{}{v, cas}, nil
}

func sget(s *Server, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqGet)

	body, err := s.Sget(r.Path)
	if err != nil {
		return nil, err
	}

	return []interface{}{body}, nil
}

func set(s *Server, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqSet)

	seqn, _, err := s.Set(r.Path, r.Body, r.Cas)
	if err != nil {
		return nil, err
	}
	return []interface{}{strconv.Uitoa64(seqn)}, nil
}

func del(s *Server, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqDel)

	_, err := s.Del(r.Path, r.Cas)
	if err != nil {
		return nil, err
	}

	return []interface{}{"true"}, nil
}

func nop(s *Server, data interface{}) (interface{}, os.Error) {
	s.Nop()
	return []interface{}{"true"}, nil
}

func join(s *Server, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqJoin)
	key := s.Prefix + "/junta/members/" + r.Who
	seqn, _, err := s.Set(key, r.Addr, store.Missing)
	if err != nil {
		return nil, err
	}

	done := make(chan int)
	go s.AdvanceUntil(done)
	s.St.Sync(seqn + uint64(s.Mg.Alpha()))
	close(done)
	seqn, snap := s.St.Snapshot()
	return []interface{}{strconv.Uitoa64(seqn), snap}, nil
}

func checkin(s *Server, data interface{}) (interface{}, os.Error) {
	r := data.(*proto.ReqCheckin)

	t, cas, err := s.Checkin(r.Sid, r.Cas)
	if err != nil {
		return nil, err
	}
	return []interface{}{strconv.Itoa64(t), cas}, nil
}

func indirect(x interface{}) interface{} {
	return reflect.Indirect(reflect.NewValue(x)).Interface()
}

type op struct {
	p interface{}
	f func(*Server, interface{}) (interface{}, os.Error)

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

			res, err := o.f(c.s, indirect(o.p))
			if err != nil {
				c.SendError(rid, err.String())
			} else {
				c.SendResponse(rid, res)
			}
			continue
		}

		rlogger.Printf("unknown command <%s>", verb)
		c.SendError(rid, proto.InvalidCommand+" "+verb)
	}
}
