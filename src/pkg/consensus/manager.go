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
	fill  vector.Vector
}


type Prop struct {
	Seqn int64
	Mut  []byte
}

var tickTemplate = &msg{Cmd: tick}
var fillTemplate = &msg{Cmd: propose, Value: []byte(store.Nop)}


func NewManager(c *Config) (m *Manager) {
	m = new(Manager)
	s := make(chan Stats)
	m.Stats = s
	m.cfg = *c
	m.run = make(map[int64]*run)
	go m.manage(s)
	return m
}


func (m *Manager) manage(statCh chan<- Stats) {
	packets := new(vector.Vector)
	ticks := new(vector.Vector)
	var nextRun int64
	var stats Stats
	runCh, err := m.cfg.Store.Wait(store.Any, m.cfg.DefRev)
	if err != nil {
		panic(err) // can't happen
	}

	for {
		stats.Runs = len(m.run)
		stats.WaitPackets = packets.Len()
		stats.WaitTicks = ticks.Len()

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
			nextRun = r.seqn + 1
			stats.TotalRuns++
			log.Println("runs:", fmtRuns(m.run))
			log.Println("avg tick delay:", avg(ticks))
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

			n = applyTriggers(packets, ticks, t, tickTemplate)
			stats.TotalTicks += int64(n)
			if n > 0 {
				log.Println("applied ticks", n)
			}
		}

		for packets.Len() > 0 {
			p := packets.At(0).(packet)
			log.Printf("p.seqn=%d nextRun=%d", *p.Seqn, nextRun)
			if *p.Seqn >= nextRun {
				break
			}
			heap.Pop(packets)

			r := m.run[*p.Seqn]
			if r == nil || r.l.done {
				go sendLearn(m.cfg.Out, p, m.cfg.Store)
			} else {
				r.update(p, ticks)
			}
		}
	}
}


func (m *Manager) propose(q heap.Interface, pr *Prop, t int64) {
	log.Println("prop", pr)
	msg := msg{Seqn: &pr.Seqn, Cmd: propose, Value: pr.Mut}
	heap.Push(q, packet{msg: msg})
	for n := pr.Seqn - 1; ; n-- {
		r := m.run[n]
		if r == nil || r.isLeader(m.cfg.Self) {
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
	r.c.size = len(r.cals)
	r.c.quor = r.quorum()
	r.c.crnd = r.indexOf(r.self) + int64(len(r.cals))
	r.l.init(int64(r.quorum()))
	m.run[r.seqn] = r
	if r.isLeader(m.cfg.Self) {
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
