package consensus


import (
	"container/heap"
	"container/vector"
	"doozer/store"
	"goprotobuf.googlecode.com/hg/proto"
	"log"
	"sort"
	"time"
)

const minviteTickTime = 1e8

var tfill int64

var UseMulti bool


type packet struct {
	Addr string
	msg
}


func (p packet) Less(y interface{}) bool {
	return *p.Seqn < *y.(packet).Seqn
}


func (p packet) String() string {
	return "packet{" + p.Addr + ", " + p.msg.String() + "}"
}


type Packet struct {
	Addr string
	Data []byte
}


type trigger struct {
	t int64 // trigger time
	n int64 // seqn
}


func (t trigger) Less(y interface{}) bool {
	return t.t < y.(trigger).t
}


type Stats struct {
	// Current queue sizes
	Runs        int
	WaitPackets int
	WaitTicks   int

	// Totals over all time
	TotalRuns  int64
	TotalFills int64
	TotalTicks int64
}


type Manager struct {
	Stats <-chan Stats
	cfg   Config
	run   map[int64]*run
	base  int64 // minimum seqn in Manager.run
	next  int64 // minimum seqn > those in Manager.run
	fill  vector.Vector
	tick  vector.Vector
	mult  run     // multi-paxos run (from most recent reconfiguration)
	mrnd  []int64 // initial values for acceptor.rnd
	mdone bool    // runs can skip phase 1
}


type Prop struct {
	Seqn int64
	Mut  []byte
}

var tickTemplate = &msg{Cmd: tick}
var fillTemplate = &msg{Cmd: propose, Value: []byte(store.Nop)}


func NewManager(c *Config) (m *Manager) {
	tfill = c.TFill
	m = new(Manager)
	s := make(chan Stats)
	m.Stats = s
	m.cfg = *c
	m.run = make(map[int64]*run)
	go m.manage(s)
	return m
}


func (m *Manager) manage(statCh chan<- Stats) {
	var minviteAt int64
	packets := new(vector.Vector)
	var stats Stats
	runCh, err := m.cfg.Store.Wait(store.Any, m.cfg.DefRev)
	if err != nil {
		panic(err) // can't happen
	}

	for {
		stats.Runs = len(m.run)
		stats.WaitPackets = packets.Len()
		stats.WaitTicks = m.tick.Len()

		select {
		case e, ok := <-runCh:
			if !ok {
				return
			}
			log.Println("event", e)

			runCh, err = m.cfg.Store.Wait(store.Any, e.Seqn+1)
			if err != nil {
				panic(err) // can't happen
			}

			m.run[e.Seqn] = nil, false
			log.Printf("del run %d", e.Seqn)
			r := m.addRun(e)
			log.Printf("add run %d", r.seqn)
			m.next = r.seqn + 1
			m.base = m.next - int64(len(m.run))
			stats.TotalRuns++
			log.Println("runs:", fmtRuns(m.run))
			log.Println("avg tick delay:", avg(&m.tick))
			log.Println("avg fill delay:", avg(&m.fill))
		case p := <-m.cfg.In:
			recvPacket(packets, p)
		case statCh <- stats:
		case pr := <-m.cfg.Props:
			m.propose(packets, pr, time.Nanoseconds())
		case t := <-m.cfg.Ticker:
			n := applyTriggers(packets, &m.fill, t, fillTemplate)
			stats.TotalFills += int64(n)
			if n > 0 {
				log.Println("applied fills", n)
			}

			n = applyTriggers(packets, &m.tick, t, tickTemplate)
			stats.TotalTicks += int64(n)
			if n > 0 {
				log.Println("applied ticks", n)
			}

			if UseMulti && !m.mdone && m.mult.seqn < m.next && t > minviteAt {
				m.mult.broadcast(&msg{
					Cmd:  newMsg_Cmd(msg_MINVITE),
					Crnd: &m.mult.c.crnd,
				})
				minviteAt = t + minviteTickTime
			}
		}

		for packets.Len() > 0 {
			p := packets.At(0).(packet)
			log.Printf("p.seqn=%d m.next=%d", *p.Seqn, m.next)
			if *p.Seqn >= m.next {
				break
			}
			heap.Pop(packets)
			switch *p.Cmd {
			case msg_MINVITE:
				m.handleMInvite(p)
			case msg_MRSVP:
				m.handleMRsvp(p)
			default:
				r := m.run[*p.Seqn]
				if r == nil || r.l.done {
					go sendLearn(m.cfg.Out, p, m.cfg.Store)
				} else {
					r.update(p, &m.tick)
				}
			}
		}
	}
}


// apply p to m.run and m.mrnd
func (m *Manager) handleMInvite(p packet) {
	if p.Crnd == nil {
		return
	}

	n := *p.Seqn
	if n < m.base {
		n = m.base
	}
	for ; n < m.next; n++ {
		r := m.run[n]
		if *p.Seqn < r.cfg {
			return
		}
		if r.iAddr(p.Addr) == r.iLeader() {
			m := r.a.update(&p.msg)
			r.broadcast(m)
		}
	}
	i := *p.Seqn % int64(len(m.mrnd))
	if *p.Seqn >= m.mult.cfg && *p.Crnd > m.mrnd[i] {
		m.mrnd[i] = *p.Crnd
	}
	b, err := proto.Marshal(&msg{
		Seqn: p.Seqn,
		Cmd:  newMsg_Cmd(msg_MRSVP),
		Crnd: p.Crnd,
	})
	if err != nil {
		log.Println(err)
		return
	}
	m.cfg.Out <- Packet{p.Addr, b}
}


func (m *Manager) handleMRsvp(p packet) {
	if p.Crnd != nil && *p.Seqn >= m.mult.cfg {
		m.mult.c.rsvps[p.Addr] = true
		if len(m.mult.c.rsvps) >= m.mult.c.quor {
			m.mdone = true
		}
	}
}


func (m *Manager) propose(q heap.Interface, pr *Prop, t int64) {
	log.Println("prop", pr)
	if m.mdone && pr.Seqn >= m.mult.seqn {
		log.Println("multi prop for", pr.Seqn, "mult.seqn is", m.mult.seqn)
		m.run[pr.Seqn].multiPropose(pr.Mut, &m.tick)
	} else {
		msg := msg{Seqn: &pr.Seqn, Cmd: propose, Value: pr.Mut}
		r := m.run[pr.Seqn]
		r.c.crnd += int64(r.c.size)
		log.Println("r.c.crnd now", r.c.crnd)
		heap.Push(q, packet{msg: msg})
	}
	for n := pr.Seqn - 1; ; n-- {
		r := m.run[n]
		if r == nil || r.iLeader() == r.iId(r.self) {
			break
		} else {
			schedTrigger(&m.fill, n, t, m.cfg.TFill)
		}
	}
}


func sendLearn(out chan<- Packet, p packet, st *store.Store) {
	if p.msg.Cmd != nil && *p.msg.Cmd == msg_INVITE {
		ch, err := st.Wait(store.Any, *p.Seqn)

		if err == store.ErrTooLate {
			log.Println(err)
		} else {
			e := <-ch
			m := msg{
				Seqn:  &e.Seqn,
				Cmd:   learn,
				Value: []byte(e.Mut),
			}
			buf, _ := proto.Marshal(&m)
			out <- Packet{p.Addr, buf}
		}
	}
}


func recvPacket(q heap.Interface, P Packet) {
	var p packet
	p.Addr = P.Addr

	err := proto.Unmarshal(P.Data, &p.msg)
	if err != nil {
		log.Println(err)
		return
	}

	if p.msg.Seqn == nil || p.msg.Cmd == nil {
		log.Printf("discarding %#v", p)
		return
	}

	log.Println("recv", p.String())
	heap.Push(q, p)
}


func avg(v *vector.Vector) (n int64) {
	t := time.Nanoseconds()
	if v.Len() == 0 {
		return -1
	}
	for _, x := range []interface{}(*v) {
		n += x.(trigger).t - t
	}
	return n / int64(v.Len())
}


func schedTrigger(q heap.Interface, n, t, tfill int64) {
	heap.Push(q, trigger{n: n, t: t + tfill})
}


func applyTriggers(packets, ticks *vector.Vector, now int64, tpl *msg) (n int) {
	for ticks.Len() > 0 {
		tt := ticks.At(0).(trigger)
		if tt.t > now {
			break
		}

		heap.Pop(ticks)

		p := packet{msg: *tpl}
		p.msg.Seqn = &tt.n
		log.Println("applying", p.msg.String())
		heap.Push(packets, p)
		n++
	}
	return
}


func (m *Manager) addRun(e store.Event) (r *run) {
	r = new(run)
	r.self = m.cfg.Self
	r.out = m.cfg.Out
	r.ops = m.cfg.Ops
	r.bound = initialWaitBound
	r.seqn = e.Seqn + m.cfg.Alpha
	r.cals = getCals(e)
	r.addr = getAddrs(e, r.cals)
	if !r.eqCals(m.run[r.seqn-1]) {
		m.mrnd = make([]int64, int64(len(r.cals)))
		m.mult.cfg = r.seqn
		m.mult.seqn = r.nextLeaderSeqn(r.self)
		m.mult.out = r.out
		m.mult.cals = r.cals
		log.Println("m", m.mult.seqn, "cals", m.mult.cals)
		m.mult.addr = r.addr
		log.Println("m", m.mult.seqn, "addr", m.mult.addr)
		m.mult.c.rsvps = make(map[string]bool)
		m.mult.c.quor = r.quorum()
		m.mult.c.crnd = r.iId(r.self) + int64(len(r.cals))
		m.mdone = false
	}
	r.cfg = m.mult.cfg
	r.a.rnd = m.mrnd[r.seqn%int64(len(r.cals))]
	r.c.size = len(r.cals)
	r.c.quor = r.quorum()
	r.c.crnd = m.mult.c.crnd
	r.l.init(int64(r.quorum()))
	m.run[r.seqn] = r
	if r.iId(r.self) == r.iLeader() {
		log.Printf("pseqn %d", r.seqn)
		m.cfg.PSeqn <- r.seqn
	}
	return r
}


func getCals(g store.Getter) []string {
	ents := store.Getdir(g, "/ctl/cal")
	cals := make([]string, len(ents))

	i := 0
	for _, cal := range ents {
		id := store.GetString(g, "/ctl/cal/"+cal)
		if id != "" {
			cals[i] = id
			i++
		}
	}

	cals = cals[0:i]
	sort.SortStrings(cals)

	return cals
}


func getAddrs(g store.Getter, cals []string) (a []string) {
	a = make([]string, len(cals))
	for i, id := range cals {
		a[i] = store.GetString(g, "/ctl/node/"+id+"/addr")
	}
	return
}


func fmtRuns(rs map[int64]*run) (s string) {
	var ns []int
	for i := range rs {
		ns = append(ns, int(i))
	}
	sort.SortInts(ns)
	for _, i := range ns {
		r := rs[int64(i)]
		if r.l.done {
			s += "X"
		} else if r.prop {
			s += "o"
		} else {
			s += "."
		}
	}
	return s
}
