package paxos

// TODO temporary name
type coord struct {
	cx   *cluster
	outs Putter

	chanPutCloser
	clock chan int
}

func newCoord(c *cluster, outs Putter) *coord {
	return &coord{
		cx:    c,
		outs:  outs,
		chanPutCloser: chanPutCloser(make(chan Msg)),
		clock: make(chan int),
	}
}

func (c *coord) Close() {
	c.chanPutCloser.Close()
	close(c.clock)
}

func (c *coord) process(target string) {
	crnd := uint64(c.cx.SelfIndex())
	if crnd == 0 {
		crnd += uint64(c.cx.Len())
	}

	var cval string

Start:
	cval = ""
	start := newInvite(crnd)
	c.outs.Put(start)

	var rsvps int
	var vr uint64
	var vv string

	for {
		select {
		case in := <-c.chanPutCloser:
			if closed(c.chanPutCloser) {
				goto Done
			}
			switch in.Cmd() {
			case rsvp:
				i, vrnd, vval := rsvpParts(in)

				if cval != "" {
					continue
				}

				if i < crnd {
					continue
				}

				if vrnd > vr {
					vr = vrnd
					vv = vval
				}

				rsvps++
				if rsvps >= c.cx.Quorum() {
					var v string

					if vr > 0 {
						v = vv
					} else {
						v = target
					}
					cval = v

					chosen := newNominate(crnd, v)
					c.outs.Put(chosen)
				}
			}
		case <-c.clock:
			if closed(c.clock) {
				goto Done
			}
			crnd += uint64(c.cx.Len())
			goto Start
		}
	}

Done:
	return
}
