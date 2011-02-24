package consensus


import (
	"container/heap"
	"container/vector"
	"goprotobuf.googlecode.com/hg/proto"
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


type Stats struct {
	Runs        int
	WaitPackets int
}


type Manager <-chan Stats


type Prop struct {
	Seqn int64
	Mut  []byte
}


// propSeqns must be buffered with capacity >= alpha
func newManager(self string, propSeqns chan<- int64, in <-chan Packet, runs <-chan *run, props <-chan *Prop) Manager {
	statCh := make(chan Stats)
	propRuns := make(chan *run)

	go filterPropSeqns(self, propRuns, propSeqns)

	go func() {
		running := make(map[int64]*run)
		packets := new(vector.Vector)
		ticks := make(chan int64)
		var nextRun int64
		var stats Stats

		for {
			stats.Runs = len(running)
			stats.WaitPackets = packets.Len()

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
				m := M{Seqn: &pr.Seqn, Cmd: propose, Value: pr.Mut}
				heap.Push(packets, packet{M: m})
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
					r.deliver(p)
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

	err := proto.Unmarshal(P.Data, &p.M)
	if err != nil {
		return
	}

	if p.M.Seqn == nil {
		return
	}

	heap.Push(q, p)
}


func schedTick(q heap.Interface, n int64) {
	heap.Push(q, packet{M: M{Seqn: proto.Int64(n), Cmd: tick}})
}
