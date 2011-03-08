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
	M
}


func (p packet) Less(y interface{}) bool {
	return *p.Seqn < *y.(packet).Seqn
}


type Packet struct {
	Addr string
	Data []byte
}


type fill struct {
	t int64 // trigger time
	n int64 // seqn
}


func (f fill) Less(y interface{}) bool {
	return f.n < y.(fill).n
}


type Stats struct {
	Runs        int
	WaitPackets int
	WaitFills   int
	Running     int
}


type Manager <-chan Stats


type Prop struct {
	Seqn int64
	Mut  []byte
}


func newManager(self string, nextFill int64, propSeqns chan<- int64, in <-chan Packet, runs <-chan *run, props <-chan *Prop, ticker <-chan int64, fillDelay int64) Manager {
	statCh := make(chan Stats)
	propRuns := make(chan *run)

	go filterPropSeqns(self, propRuns, propSeqns)

	go func() {
		running := make(map[int64]*run)
		packets := new(vector.Vector)
		fills := new(vector.Vector)
		ticks := make(chan int64)
		var nextRun int64
		var stats Stats

		for {
			stats.Runs = len(running)
			stats.WaitPackets = packets.Len()
			stats.WaitFills = fills.Len()
			stats.Running = len(running)

			select {
			case run := <-runs:
				if closed(runs) {
					return
				}

				running[run.seqn] = run
				nextRun = run.seqn + 1
				run.ticks = ticks
				propRuns <- run
			case p := <-in:
				recvPacket(packets, p)
			case n := <-ticks:
				schedTick(packets, n)
			case statCh <- stats:
			case pr := <-props:
				log.Printf("propose seqn=%d", pr.Seqn)
				m := M{Seqn: &pr.Seqn, Cmd: propose, Value: pr.Mut}
				heap.Push(packets, packet{M: m})

				for nextFill < pr.Seqn {
					schedFill(fills, nextFill, fillDelay)
					nextFill++
				}
				nextFill++
			case t := <-ticker:
				for fills.Len() > 0 {
					f := fills.At(0).(fill)

					if f.n >= t {
						break
					}

					heap.Pop(fills)

					m := M{Seqn: &f.n, Cmd: propose, Value: []byte(store.Nop)}
					heap.Push(packets, packet{M: m})
				}
			}

			for packets.Len() > 0 {
				p := packets.At(0).(packet)

				seqn := *p.Seqn

				if seqn >= nextRun {
					break
				}

				heap.Pop(packets)

				r := running[seqn]
				if r != nil {
					if r.deliver(p) {
						running[seqn] = nil, false
					}
				}
			}
		}
	}()

	return statCh
}


func filterPropSeqns(id string, rc <-chan *run, sc chan<- int64) {
	for r := range rc {
		if r.isLeader(id) {
			sc <- r.seqn
		}
	}
}


func recvPacket(q heap.Interface, P Packet) {
	var p packet
	p.Addr = P.Addr

	err := proto.Unmarshal(P.Data, &p.M)
	if err != nil {
		return
	}

	if p.M.Seqn == nil {
		return
	}

	heap.Push(q, p)
}


func schedFill(q heap.Interface, n, fillDelay int64) {
	heap.Push(q, fill{n: n, t: time.Nanoseconds() + fillDelay})
}


func schedTick(q heap.Interface, n int64) {
	heap.Push(q, packet{M: M{Seqn: proto.Int64(n), Cmd: tick}})
}
