package paxos

// TODO temporary name
type C struct {
	cx   *cluster
	outs Putter

	ins   chan Msg
	clock chan int
}

func NewC(c *cluster, outs Putter) *C {
	return &C{
		cx:    c,
		outs:  outs,
		ins:   make(chan Msg),
		clock: make(chan int),
	}
}

func (c *C) Put(m Msg) {
	go func() {
		c.ins <- m
	}()
}

func (c *C) Close() {
	close(c.ins)
	close(c.clock)
}

func (c *C) process(target string) {
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
		case in := <-c.ins:
			if closed(c.ins) {
				goto Done
			}
			switch in.Cmd() {
			case Rsvp:
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
