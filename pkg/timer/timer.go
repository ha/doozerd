package timer

import (
	"container/heap"
	"container/vector"
	"junta/store"
	"junta/util"
	"math"
	"strconv"
	"time"
)

const (
	OneMicrosecond = 1e3 // ns
	OneMillisecond = 1e6 // ns
	OneSecond      = 1e9 // ns
)

type Tick struct {
	Path string
	At   int64
}


func (t Tick) Less(y interface{}) bool {
	return t.At < y.(Tick).At
}

type Timer struct {
	Pattern string

	// Ticks are sent here
	C chan Tick

	events  chan store.Event
	lengths chan int
	ticker  *time.Ticker
}

func New(pattern string, interval int64, st *store.Store) *Timer {
	t := &Timer{
		Pattern: pattern,
		C:       make(chan Tick),
		events:  make(chan store.Event),
		lengths: make(chan int),
		ticker:  time.NewTicker(interval),
	}

	// Begin watching as timers come and go
	st.Watch(pattern, t.events)

	go t.process()

	return t
}

func (t *Timer) process() {
	logger := util.NewLogger("timer (%s)", t.Pattern)

	ticks := new(vector.Vector)

	peek := func() Tick {
		if ticks.Len() == 0 {
			return Tick{At: math.MaxInt64}
		}
		return ticks.At(0).(Tick)
	}

	for {
		select {
		case e := <-t.events:
			if closed(t.events) {
				goto done
			}

			logger.Printf("recvd: %v", e)
			// TODO: Handle/Log the next error
			// I'm not sure if we should notify the client
			// on Set.  That seems like it would be difficult
			// with the currect way the code functions.  Dunno.
			at, _ := strconv.Atoi64(e.Body)

			x := Tick{e.Path, at}
			switch {
			default:
				break
			case e.IsSet():
				// First remove it if it's already there.
				for i := 0; i < ticks.Len(); i++ {
					if ticks.At(i).(Tick).Path == x.Path {
						heap.Remove(ticks, i)
						i = 0 // have to start over; heap could be reordered
					}
				}

				heap.Push(ticks, x)
			case e.IsDel():
				logger.Println("deleting", e.Path, e.Body)
				// This could be optimize since ticks is sorted; I can't
				// find a way without implementing our own quick-find.
				for i := 0; i < ticks.Len(); i++ {
					if ticks.At(i).(Tick).Path == x.Path {
						heap.Remove(ticks, i)
						i = 0 // have to start over; heap could be reordered
					}
				}
			}

		case ns := <-t.ticker.C:
			for next := peek(); next.At <= ns; next = peek() {
				logger.Printf("ticked %#v", next)
				heap.Pop(ticks)
				t.C <- next
			}
		case t.lengths <- ticks.Len():
			// pass
		}
	}

done:
	t.ticker.Stop()
}

func (t *Timer) Len() int {
	return <-t.lengths
}

func (t *Timer) Close() {
	close(t.events)
}
