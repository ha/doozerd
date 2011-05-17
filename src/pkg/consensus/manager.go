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
	WaitFills   int
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
	fills := new(vector.Vector)
	ticks := new(vector.Vector)
	nextFill := m.cfg.DefRev + m.cfg.Alpha
	var nextRun int64
	var stats Stats
	runCh, err := m.cfg.Store.Wait(store.Any, m.cfg.DefRev)
	if err != nil {
		panic(err) // can't happen
	}

	for {
		stats.Runs = len(m.run)
		stats.WaitPackets = packets.Len()
		stats.WaitFills = fills.Len()
		stats.WaitTicks = ticks.Len()

		select {
		case e, ok := <-runCh:
			if !ok {
				return
			}

			runCh, err = m.cfg.Store.Wait(store.Any, e.Seqn+1)
			if err != nil {
				panic(err) // can't happen
			}

			m.run[e.Seqn] = nil, false
			nextRun = m.addRun(e).seqn + 1
			stats.TotalRuns++
		case p := <-m.cfg.In:
			recvPacket(packets, p)
		case statCh <- stats:
		case pr := <-m.cfg.Props:
			log.Printf("propose seqn=%d", pr.Seqn)
			msg := msg{Seqn: &pr.Seqn, Cmd: propose, Value: pr.Mut}
			heap.Push(packets, packet{msg: msg})

			for nextFill < pr.Seqn {
				schedTrigger(fills, nextFill, m.cfg.TFill)
				nextFill++
			}
			nextFill++
		case t := <-m.cfg.Ticker:
			n := applyTriggers(packets, fills, t, fillTemplate)
			stats.TotalFills += int64(n)

			n = applyTriggers(packets, ticks, t, tickTemplate)
			stats.TotalTicks += int64(n)
		}

		for packets.Len() > 0 {
			p := packets.At(0).(packet)
			if *p.Seqn >= nextRun {
				break
			}
			heap.Pop(packets)

			if r := m.run[*p.Seqn]; r != nil {
				if r.l.done {
					go sendLearn(m.cfg.Out, p, m.cfg.Store)
				} else {
					r.update(p, ticks)
				}
			}
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
		return
	}

	if p.msg.Seqn == nil {
		return
	}

	heap.Push(q, p)
}


func schedTrigger(q heap.Interface, n, tfill int64) {
	heap.Push(q, trigger{n: n, t: time.Nanoseconds() + tfill})
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
	r.addrs = getAddrs(e, r.cals)
	r.c.size = len(r.cals)
	r.c.quor = r.quorum()
	r.c.crnd = r.indexOf(r.self) + int64(len(r.cals))
	r.l.init(int64(r.quorum()))
	m.run[r.seqn] = r
	if r.isLeader(m.cfg.Self) {
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


func getAddrs(g store.Getter, cals []string) map[string]bool {
	addrs := make(map[string]bool)

	for _, id := range cals {
		addrs[store.GetString(g, "/ctl/node/"+id+"/addr")] = true
	}

	return addrs
}
