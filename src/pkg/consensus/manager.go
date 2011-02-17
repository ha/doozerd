package consensus


import (
	"container/heap"
	"container/vector"
)



type Packet struct {
	M
	Addr string
}


func (p Packet) Less(y interface{}) bool {
	return *p.WireSeqn < *y.(Packet).WireSeqn
}


type Stats struct {
	Runs int
	WaitPackets int
}


type Manager <-chan Stats


func NewManager(in <-chan Packet, out chan<- Packet, runs <-chan *Run) Manager {
	stats := make(chan Stats)
	running := make(map[int64]*Run)
	packets := new(vector.Vector)

	var nextRun int64

	go func() {
		for {
			select {
			case run := <-runs:
				running[run.Seqn] = run
				nextRun = run.Seqn+1
			case p := <-in:
				heap.Push(packets, p)
			case stats <- Stats{len(running), packets.Len()}:
			}

			for packets.Len() > 0 {
				p := packets.At(0).(Packet)

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


