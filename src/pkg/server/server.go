package server

import (
	dnet "doozer/net"
	"doozer/paxos"
	"doozer/proto"
	"doozer/store"
	"doozer/util"
	"net"
	"os"
	"rand"
	"reflect"
	"strconv"
	"time"
)

import "log"

const packetSize = 3000

const lease = 3e9 // ns == 3s

var (
	ErrNoWrite = os.NewError("no known writeable address")
	responded  = os.NewError("already responded")
)

const (
	Ok = proto.Line("OK")
)

type conn struct {
	*proto.Conn
	c   net.Conn
	s   *Server
	cal bool
}

type Manager interface {
	paxos.Proposer
	ProposeOnce(v string) store.Event
	PutFrom(string, paxos.Msg)
	Alpha() int
}

type Server struct {
	Conn net.PacketConn
	Addr string
	St   *store.Store
	Mg   Manager
	Self string
}

func (sv *Server) ServeUdp(outs chan paxos.Packet) {
	r := dnet.Ackify(sv.Conn, outs)

	for p := range r {
		sv.Mg.PutFrom(p.Addr, p.Msg)
	}
}

var clg = util.NewLogger("cal")

func (s *Server) Serve(l net.Listener, cal chan int) os.Error {
	for {
		rw, err := l.Accept()
		if err != nil {
			log.Printf("%#v", err)
			if e, ok := err.(*net.OpError); ok && e.Error == os.EINVAL {
				return nil
			}
			return err
		}
		c := &conn{proto.NewConn(rw), rw, s, closed(cal)}
		go c.serve()
	}

	panic("unreachable")
}

func (sv *Server) cals() []string {
	parts, cas := sv.St.Get("/doozer/leader")
	if cas == store.Dir && cas == store.Missing {
		return nil
	}
	return parts
}

// Repeatedly propose nop values until a successful read from `done`.
func (sv *Server) AdvanceUntil(done chan int) {
	for _, ok := <-done; !ok; _, ok = <-done {
		sv.Mg.Propose(store.Nop)
	}
}

func (c *conn) redirect(rid uint) {
	cals := c.s.cals()
	if len(cals) < 1 {
		c.SendResponse(rid, proto.Last, ErrNoWrite)
		return
	}
	cal := cals[rand.Intn(len(cals))]
	parts, cas := c.s.St.Get("/doozer/info/" + cal + "/public-addr")
	if cas == store.Dir && cas == store.Missing {
		c.SendResponse(rid, proto.Last, ErrNoWrite)
		return
	}
	c.SendResponse(rid, proto.Last, proto.Redirect(parts[0]))
}

func get(c *conn, _ uint, data interface{}) interface{} {
	r := data.(*proto.ReqGet)
	v, cas := c.s.St.Get(r.Path)
	return proto.ResGet{v, cas}
}

func sget(c *conn, _ uint, data interface{}) interface{} {
	r := data.(*proto.ReqGet)
	return store.GetString(c.s.St.SyncPath(r.Path), r.Path)
}

func set(c *conn, _ uint, data interface{}) interface{} {
	r := data.(*proto.ReqSet)
	_, cas, err := paxos.Set(c.s.Mg, r.Path, r.Body, r.Cas)
	if err != nil {
		return err
	}
	return cas
}

func del(c *conn, _ uint, data interface{}) interface{} {
	r := data.(*proto.ReqDel)
	err := paxos.Del(c.s.Mg, r.Path, r.Cas)
	if err != nil {
		return err
	}

	return Ok
}

func noop(c *conn, _ uint, data interface{}) interface{} {
	c.s.Mg.ProposeOnce(store.Nop)
	return Ok
}

func join(c *conn, _ uint, data interface{}) interface{} {
	r := data.(*proto.ReqJoin)
	key := "/doozer/members/" + r.Who
	seqn, _, err := paxos.Set(c.s.Mg, key, r.Addr, store.Missing)
	if err != nil {
		return err
	}

	done := make(chan int)
	go c.s.AdvanceUntil(done)
	c.s.St.Sync(seqn + uint64(c.s.Mg.Alpha()))
	close(done)
	seqn, snap := c.s.St.Snapshot()
	return proto.ResJoin{seqn, snap}
}

func sett(c *conn, _ uint, data interface{}) interface{} {
	r := data.(*proto.ReqSett)
	t := time.Nanoseconds() + r.Interval
	_, cas, err := paxos.Set(c.s.Mg, r.Path, strconv.Itoa64(t), r.Cas)
	if err != nil {
		return err
	}
	return proto.ResSett{t, cas}
}

func checkin(c *conn, _ uint, data interface{}) interface{} {
	r := data.(*proto.ReqCheckin)
	t := time.Nanoseconds() + lease
	_, cas, err := paxos.Set(c.s.Mg, "/session/"+r.Sid, strconv.Itoa64(t), r.Cas)
	if err != nil {
		return err
	}
	return proto.ResCheckin{t, cas}
}

func closeOp(c *conn, _ uint, data interface{}) interface{} {
	err := c.CloseResponse(data.(uint))
	if err != nil {
		return err
	}
	return Ok
}

func watch(c *conn, id uint, data interface{}) interface{} {
	glob := data.(string)
	// TODO check glob pattern for errors
	ch := c.s.St.Watch(glob)
	// TODO buffer (and possibly discard) events
	for ev := range ch {
		var r proto.ResWatch
		r.Path = ev.Path
		r.Body = ev.Body
		r.Cas = ev.Cas
		err := c.SendResponse(id, 0, r)
		if err == proto.ErrClosed {
			close(ch)
			err = nil
		}
		if err != nil {
			// TODO log error
			break
		}
	}
	return responded
}

func indirect(x interface{}) interface{} {
	return reflect.Indirect(reflect.NewValue(x)).Interface()
}

type handler func(*conn, uint, interface{}) interface{}

type op struct {
	p interface{}
	f handler

	redirect bool
}

var ops = map[string]op{
	// new stuff, see doc/proto.md
	"CLOSE": {p: new(uint), f: closeOp},
	"DEL":   {p: new(*proto.ReqDel), f: del, redirect: true},
	"NOOP":  {p: new(interface{}), f: noop, redirect: true},
	"SET":   {p: new(*proto.ReqSet), f: set, redirect: true},
	"SETT":  {p: new(*proto.ReqSett), f: sett, redirect: true},
	"WATCH": {p: new(string), f: watch},

	// former stuff
	"get":     {p: new(*proto.ReqGet), f: get},
	"sget":    {p: new(*proto.ReqGet), f: sget},
	"join":    {p: new(*proto.ReqJoin), f: join, redirect: true},
	"checkin": {p: new(*proto.ReqCheckin), f: checkin, redirect: true},
}

func (c *conn) handle(rid uint, f handler, data interface{}) {
	res := f(c, rid, data)
	if res == responded {
		return
	}

	c.SendResponse(rid, proto.Last, res)
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
				c.SendResponse(rid, proto.Last, err)
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
		c.SendResponse(rid, proto.Last, os.ErrorString(proto.InvalidCommand+" "+verb))
	}
}
