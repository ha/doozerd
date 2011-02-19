package consensus


import (
	"container/heap"
	"container/vector"
	"doozer/store"
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


func NewManager(in <-chan Packet, out chan<- packet, runs <-chan *run, ops chan<- store.Op) Manager {
	statCh := make(chan Stats)
	running := make(map[int64]*run)
	packets := new(vector.Vector)
	ticks := make(chan int64)

	var nextRun int64

	go func() {
		var stats Stats
		for {
			stats.Runs = len(running)
			stats.WaitPackets = packets.Len()

			select {
			case run := <-runs:
				running[run.seqn] = run
				nextRun = run.seqn + 1
				run.ops = ops
				run.ticks = ticks
			case p := <-in:
				recvPacket(packets, p)
			case statCh <- stats:
			}

			for packets.Len() > 0 {
				p := packets.At(0).(packet)

				seqn := *p.Seqn

				if seqn >= nextRun {
					break
				}

				heap.Pop(packets)

				running[seqn].deliver(p)
			}
		}
	}()

	return statCh
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
