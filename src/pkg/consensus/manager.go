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


type Manager <-chan Stats


type Prop struct {
	Seqn int64
	Mut  []byte
}

var tickTemplate = &msg{Cmd: tick}
var fillTemplate = &msg{Cmd: propose, Value: []byte(store.Nop)}


func NewManager(c *Config) Manager {
	stat := make(chan Stats)
	go manage(c, stat)
	return stat
}


func manage(c *Config, statCh chan Stats) {
	runs := make(map[int64]*run)
	packets := new(vector.Vector)
	fills := new(vector.Vector)
	ticks := new(vector.Vector)
	nextFill := c.DefRev + c.Alpha
	var nextRun int64
	var stats Stats
	runCh, err := c.Store.Wait(store.Any, c.DefRev)
	if err != nil {
		panic(err) // can't happen
	}

	for {
		stats.Runs = len(runs)
		stats.WaitPackets = packets.Len()
		stats.WaitFills = fills.Len()
		stats.WaitTicks = ticks.Len()

		select {
		case e, ok := <-runCh:
			if !ok {
				return
			}

			runCh, err = c.Store.Wait(store.Any, e.Seqn+1)
			if err != nil {
				panic(err) // can't happen
			}

			runs[e.Seqn] = nil, false
			nextRun = addRun(c, runs, e).seqn + 1
			stats.TotalRuns++
		case p := <-c.In:
			recvPacket(packets, p)
		case statCh <- stats:
		case pr := <-c.Props:
			log.Printf("propose seqn=%d", pr.Seqn)
			m := msg{Seqn: &pr.Seqn, Cmd: propose, Value: pr.Mut}
			heap.Push(packets, packet{msg: m})

			for nextFill < pr.Seqn {
				schedTrigger(fills, nextFill, c.TFill)
				nextFill++
			}
			nextFill++
		case t := <-c.Ticker:
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

			if r := runs[*p.Seqn]; r != nil {
				if r.l.done {
					go sendLearn(c.Out, p, c.Store)
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


func addRun(c *Config, runs map[int64]*run, e store.Event) (r *run) {
	r = new(run)
	r.self = c.Self
	r.out = c.Out
	r.ops = c.Ops
	r.bound = initialWaitBound
	r.seqn = e.Seqn + c.Alpha
	r.cals = getCals(e)
	r.addrs = getAddrs(e)
	r.c.size = len(r.cals)
	r.c.quor = r.quorum()
	r.c.crnd = r.indexOf(r.self) + int64(len(r.cals))
	r.l.init(int64(r.quorum()))
	runs[r.seqn] = r
	if r.isLeader(c.Self) {
		c.PSeqn <- r.seqn
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


func getAddrs(g store.Getter) map[string]bool {
	// TODO include only CALs, once followers use TCP for updates.

	ids := store.Getdir(g, "/ctl/node")
	addrs := make(map[string]bool)

	for _, id := range ids {
		addrs[store.GetString(g, "/ctl/node/"+id+"/addr")] = true
	}

	return addrs
}
