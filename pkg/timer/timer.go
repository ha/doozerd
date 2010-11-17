package timer

import (
	"container/heap"
	"container/vector"
	"doozer/store"
	"doozer/util"
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
	C <-chan Tick

	events  chan store.Event

	ticks   *vector.Vector
	ticker  *time.Ticker
}

func New(pattern string, interval int64, st *store.Store) *Timer {
	c := make(chan Tick)
	t := &Timer{
		Pattern: pattern,
		C:       c,
		events:  make(chan store.Event),
		ticks:   new(vector.Vector),
		ticker:  time.NewTicker(interval),
	}

	// Begin watching as timers come and go
	st.Watch(pattern, t.events)

	go t.process(c)

	return t
}

func (t *Timer) process(c chan Tick) {
	defer close(c)

	defer t.ticker.Stop()

	logger := util.NewLogger("timer (%s)", t.Pattern)

	peek := func() Tick {
		if t.ticks.Len() == 0 {
			return Tick{At: math.MaxInt64}
		}
		return t.ticks.At(0).(Tick)
	}

	for {
		select {
		case e := <-t.events:
			if closed(t.events) {
				return
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
				for i := 0; i < t.ticks.Len(); i++ {
					if t.ticks.At(i).(Tick).Path == x.Path {
						heap.Remove(t.ticks, i)
						i = 0 // have to start over; heap could be reordered
					}
				}

				heap.Push(t.ticks, x)
			case e.IsDel():
				logger.Println("deleting", e.Path, e.Body)
				// This could be optimize since t.ticks is sorted; I can't
				// find a way without implementing our own quick-find.
				for i := 0; i < t.ticks.Len(); i++ {
					if t.ticks.At(i).(Tick).Path == x.Path {
						heap.Remove(t.ticks, i)
						i = 0 // have to start over; heap could be reordered
					}
				}
			}

		case ns := <-t.ticker.C:
			for next := peek(); next.At <= ns; next = peek() {
				logger.Printf("ticked %#v", next)
				heap.Pop(t.ticks)
				c <- next
			}
		}
	}
}

func (t *Timer) Close() {
	close(t.events)
}
