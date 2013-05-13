package consensus

import (
	"code.google.com/p/goprotobuf/proto"
	"container/heap"
	"github.com/ha/doozerd/store"
	"log"
	"net"
	"sort"
	"time"
)

type packet struct {
	Addr *net.UDPAddr
	msg
}

type packets []*packet

func (p *packets) Len() int {
	return len(*p)
}

func (p *packets) Less(i, j int) bool {
	a := *p
	return *a[i].Seqn < *a[j].Seqn
}

func (p *packets) Push(x interface{}) {
	*p = append(*p, x.(*packet))
}

func (p *packets) Pop() (x interface{}) {
	a := *p
	i := len(a) - 1
	*p, x = a[:i], a[i]
	return
}

func (p *packets) Swap(i, j int) {
	a := *p
	a[i], a[j] = a[j], a[i]
}

type Packet struct {
	Addr *net.UDPAddr
	Data []byte
}

type trigger struct {
	t int64 // trigger time
	n int64 // seqn
}

type triggers []trigger

func (t *triggers) Len() int {
	return len(*t)
}

func (t *triggers) Less(i, j int) bool {
	a := *t
	if a[i].t == a[j].t {
		return a[i].n < a[j].n
	}
	return a[i].t < a[j].t
}

func (t *triggers) Push(x interface{}) {
	*t = append(*t, x.(trigger))
}

func (t *triggers) Pop() (x interface{}) {
	a := *t
	i := len(a) - 1
	*t, x = a[:i], a[i]
	return nil
}

func (t *triggers) Swap(i, j int) {
	a := *t
	a[i], a[j] = a[j], a[i]
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
	TotalRecv  [nmsg]int64
}

// DefRev is the rev in which this manager was defined;
// it will participate starting at DefRev+Alpha.
type Manager struct {
	Self   string
	DefRev int64
	Alpha  int64
	In     <-chan Packet
	Out    chan<- Packet
	Ops    chan<- store.Op
	PSeqn  chan<- int64
	Props  <-chan *Prop
	TFill  int64
	Store  *store.Store
	Ticker <-chan time.Time
	Stats  Stats
	run    map[int64]*run
	next   int64 // unused seqn
	fill   triggers
	packet packets
	tick   triggers
}

type Prop struct {
	Seqn int64
	Mut  []byte
}

var tickTemplate = &msg{Cmd: tick}
var fillTemplate = &msg{Cmd: propose, Value: []byte(store.Nop)}

func (m *Manager) Run() {
	m.run = make(map[int64]*run)
	runCh, err := m.Store.Wait(store.Any, m.DefRev)
	if err != nil {
		panic(err) // can't happen
	}

	for {
		m.Stats.Runs = len(m.run)
		m.Stats.WaitPackets = len(m.packet)
		m.Stats.WaitTicks = len(m.tick)

		select {
		case e, ok := <-runCh:
			if !ok {
				return
			}
			log.Println("event", e)

			runCh, err = m.Store.Wait(store.Any, e.Seqn+1)
			if err != nil {
				panic(err) // can't happen
			}

			m.event(e)
			m.Stats.TotalRuns++
			log.Println("runs:", fmtRuns(m.run))
			log.Println("avg tick delay:", avg(m.tick))
			log.Println("avg fill delay:", avg(m.fill))
		case p := <-m.In:
			if p1 := recvPacket(&m.packet, p); p1 != nil {
				m.Stats.TotalRecv[*p1.msg.Cmd]++
			}
		case pr := <-m.Props:
			m.propose(&m.packet, pr, time.Now().UnixNano())
		case t := <-m.Ticker:
			m.doTick(t.UnixNano())
		}

		m.pump()
	}
}

func (m *Manager) pump() {
	for len(m.packet) > 0 {
		p := m.packet[0]
		log.Printf("p.seqn=%d m.next=%d", *p.Seqn, m.next)
		if *p.Seqn >= m.next {
			break
		}
		heap.Pop(&m.packet)

		r := m.run[*p.Seqn]
		if r == nil || r.l.done {
			go sendLearn(m.Out, p, m.Store)
		} else {
			r.update(p, r.indexOfAddr(p.Addr), &m.tick)
		}
	}
}

func (m *Manager) doTick(t int64) {
	n := applyTriggers(&m.packet, &m.fill, t, fillTemplate)
	m.Stats.TotalFills += int64(n)
	if n > 0 {
		log.Println("applied fills", n)
	}

	n = applyTriggers(&m.packet, &m.tick, t, tickTemplate)
	m.Stats.TotalTicks += int64(n)
	if n > 0 {
		log.Println("applied m.tick", n)
	}
}

func (m *Manager) propose(q heap.Interface, pr *Prop, t int64) {
	log.Println("prop", pr)
	p := new(packet)
	p.msg.Seqn = &pr.Seqn
	p.msg.Cmd = propose
	p.msg.Value = pr.Mut
	heap.Push(q, p)
	for n := pr.Seqn - 1; ; n-- {
		r := m.run[n]
		if r == nil || r.isLeader(m.Self) {
			break
		} else {
			schedTrigger(&m.fill, n, t, m.TFill)
		}
	}
}

func sendLearn(out chan<- Packet, p *packet, st *store.Store) {
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

func recvPacket(q heap.Interface, P Packet) (p *packet) {
	p = new(packet)
	p.Addr = P.Addr

	err := proto.Unmarshal(P.Data, &p.msg)
	if err != nil {
		log.Println(err)
		return nil
	}

	if p.msg.Seqn == nil || p.msg.Cmd == nil {
		log.Printf("discarding %#v", p)
		return nil
	}

	heap.Push(q, p)
	return p
}

func avg(v []trigger) (n int64) {
	t := time.Now().UnixNano()
	if len(v) == 0 {
		return -1
	}
	for _, x := range v {
		n += x.t - t
	}
	return n / int64(len(v))
}

func schedTrigger(q heap.Interface, n, t, tfill int64) {
	heap.Push(q, trigger{n: n, t: t + tfill})
}

func applyTriggers(ps *packets, ticks *triggers, now int64, tpl *msg) (n int) {
	for ticks.Len() > 0 {
		tt := (*ticks)[0]
		if tt.t > now {
			break
		}

		heap.Pop(ticks)

		p := new(packet)
		p.msg = *tpl
		p.msg.Seqn = &tt.n
		log.Println("applying", *p.Seqn, msg_Cmd_name[int32(*p.Cmd)])
		heap.Push(ps, p)
		n++
	}
	return
}

func (m *Manager) event(e store.Event) {
	delete(m.run, e.Seqn)
	log.Printf("del run %d", e.Seqn)
	m.addRun(e)
}

func (m *Manager) addRun(e store.Event) (r *run) {
	r = new(run)
	r.self = m.Self
	r.out = m.Out
	r.ops = m.Ops
	r.bound = initialWaitBound
	r.seqn = e.Seqn + m.Alpha
	r.cals = getCals(e)
	r.addr = getAddrs(e, r.cals)
	if len(r.cals) < 1 {
		r.cals = m.run[r.seqn-1].cals
		r.addr = m.run[r.seqn-1].addr
	}
	r.c.size = len(r.cals)
	r.c.quor = r.quorum()
	r.c.crnd = r.indexOf(r.self) + int64(len(r.cals))
	r.l.init(len(r.cals), int64(r.quorum()))
	m.run[r.seqn] = r
	if r.isLeader(m.Self) {
		log.Printf("pseqn %d", r.seqn)
		m.PSeqn <- r.seqn
	}
	log.Printf("add run %d", r.seqn)
	m.next = r.seqn + 1
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
	sort.Strings(cals)

	return cals
}

func getAddrs(g store.Getter, cals []string) (a []*net.UDPAddr) {
	a = make([]*net.UDPAddr, len(cals))
	var i int
	var err error
	for _, id := range cals {
		s := store.GetString(g, "/ctl/node/"+id+"/addr")
		a[i], err = net.ResolveUDPAddr("udp", s)
		if err != nil {
			log.Println(err)
		} else {
			i++
		}
	}
	return a[:i]
}

func fmtRuns(rs map[int64]*run) (s string) {
	var ns []int
	for i := range rs {
		ns = append(ns, int(i))
	}
	sort.Ints(ns)
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
