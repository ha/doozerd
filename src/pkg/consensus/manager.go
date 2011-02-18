package consensus


import (
	"container/heap"
	"container/vector"
)


type packet struct {
	Addr string
	M
}


func (p packet) Less(y interface{}) bool {
	return *p.WireSeqn < *y.(packet).WireSeqn
}


type Stats struct {
	Runs        int
	WaitPackets int
}


type Manager <-chan Stats


func NewManager(in <-chan packet, out chan<- packet, runs <-chan *run) Manager {
	stats := make(chan Stats)
	running := make(map[int64]*run)
	packets := new(vector.Vector)

	var nextRun int64

	go func() {
		for {
			select {
			case run := <-runs:
				running[run.seqn] = run
				nextRun = run.seqn + 1
			case p := <-in:
				heap.Push(packets, p)
			case stats <- Stats{len(running), packets.Len()}:
			}

			for packets.Len() > 0 {
				p := packets.At(0).(packet)

				seqn := *p.WireSeqn

				if seqn >= nextRun {
					break
				}

				heap.Pop(packets)

				running[seqn].Deliver(p)
			}
		}
	}()

	return stats
}
