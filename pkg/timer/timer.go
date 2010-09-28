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
	oneMillisecond = 1e3 // ns
	oneMicrosecond = 1e6 // ns
	oneSecond      = 1e9 // ns
)

const (
	timerMatch = "/timer/**"
)


type Tick struct {
	Path string
	At   int64
}


func (t Tick) Less(y interface{}) bool {
	return t.At < y.(Tick).At
}

type length chan int

type Timer struct {
	Name string

	// Ticks are sent here
	C chan Tick

	events chan store.Event
	lengths chan length
	ticker *time.Ticker
}

func New(name string, interval int64, st *store.Store) *Timer {
	t := &Timer{
		Name:   name,
		C:      make(chan Tick),
		events: make(chan store.Event),
		lengths: make(chan length),
		ticker: time.NewTicker(interval),
	}

	// Begin watching as timers come and go
	st.Watch(timerMatch, t.events)

	go t.process()

	return t
}

func (t *Timer) process() {
	logger := util.NewLogger("timer (%s)", t.Name)

	ticks := new(vector.Vector)
	heap.Init(ticks)

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

			logger.Logf("recvd: %v", e)
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
				heap.Push(ticks, x)
			case e.IsDel():
				logger.Logf("deleting: %#v", e)
				// This could be optimize since ticks is sorted; I can't
				// find a way without implementing our own quick-find.
				for i := 0; i < ticks.Len(); i++ {
					if ticks.At(i).(Tick).Path == x.Path {
						heap.Remove(ticks, i)
					}
				}
			}

		case <-t.ticker.C:
			ns := time.Nanoseconds()
			for next := peek(); next.At <= ns; next = peek() {
				logger.Logf("ticked %#v", next)
				heap.Pop(ticks)
				t.C <- next
			}
		case ch := <-t.lengths:
			ch <- ticks.Len()
		}
	}

done:
	t.ticker.Stop()
}

func (t *Timer) Len() int {
	ch := make(chan int)
	t.lengths <- ch
	return <-ch
}

func (t *Timer) Close() {
	close(t.events)
}
