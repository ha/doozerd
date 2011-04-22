package consensus


import (
	"container/heap"
	"container/vector"
	"doozer/store"
	"goprotobuf.googlecode.com/hg/proto"
	"log"
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
	Running     int

	// Totals over all time
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

func newManager(self string, nextFill int64, propSeqns chan<- int64, in <-chan Packet, runs <-chan *run, props <-chan *Prop, ticker <-chan int64, fillDelay int64, st *store.Store, out chan<- Packet) Manager {
	statCh := make(chan Stats)

	go func() {
		running := make(map[int64]*run)
		packets := new(vector.Vector)
		fills := new(vector.Vector)
		ticks := new(vector.Vector)
		var nextRun int64
		var stats Stats

		for {
			stats.Runs = len(running)
			stats.WaitPackets = packets.Len()
			stats.WaitFills = fills.Len()
			stats.WaitTicks = ticks.Len()
			stats.Running = len(running)

			select {
			case run := <-runs:
				if closed(runs) {
					return
				}

				running[run.seqn] = run
				nextRun = run.seqn + 1
				if run.isLeader(self) {
					propSeqns <- run.seqn
				}
			case p := <-in:
				recvPacket(packets, p)
			case statCh <- stats:
			case pr := <-props:
				log.Printf("propose seqn=%d", pr.Seqn)
				m := msg{Seqn: &pr.Seqn, Cmd: propose, Value: pr.Mut}
				heap.Push(packets, packet{msg: m})

				for nextFill < pr.Seqn {
					schedTrigger(fills, nextFill, fillDelay)
					nextFill++
				}
				nextFill++
			case t := <-ticker:
				n := applyTriggers(packets, fills, t, fillTemplate)
				stats.TotalFills += int64(n)

				n = applyTriggers(packets, ticks, t, tickTemplate)
				stats.TotalTicks += int64(n)
			}

			for packets.Len() > 0 {
				p := packets.At(0).(packet)

				seqn := *p.Seqn

				if seqn >= nextRun {
					break
				}

				heap.Pop(packets)

				r := running[seqn]
				if r == nil {
					go sendLearn(out, p, st)
					continue
				}

				learned := r.update(p, ticks)
				if learned {
					running[seqn] = nil, false
				}
			}
		}
	}()

	return statCh
}


func sendLearn(out chan<- Packet, p packet, st *store.Store) {
	if p.msg.Cmd != nil && *p.msg.Cmd == msg_INVITE {
		ch, err := st.Wait(*p.Seqn)

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


func schedTrigger(q heap.Interface, n, fillDelay int64) {
	heap.Push(q, trigger{n: n, t: time.Nanoseconds() + fillDelay})
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
