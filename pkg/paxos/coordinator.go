package paxos

type coordinator struct {
	cx   *cluster
	outs Putter

	begun  bool
	target string
	crnd   uint64
	cval   string
	rsvps  int
	vr     uint64
	vv     string

	seen   uint64
}

func (co *coordinator) Put(in Msg) {
	if co.crnd == 0 {
		co.crnd += uint64(co.cx.Len())
	}

	switch in.Cmd() {
	case propose:
		if co.begun {
			break
		}

		co.begun = true
		co.target = proposeParts(in)
		co.outs.Put(newInvite(co.crnd))
		co.vr = 0
		co.vv = ""
		co.rsvps = 0
		co.cval = ""
	case rsvp:
		if !co.begun {
			break
		}

		i, vrnd, vval := rsvpParts(in)

		if co.cval != "" {
			break
		}

		if i > co.seen {
			co.seen = i
		}

		if i != co.crnd {
			break
		}

		if vrnd > co.vr {
			co.vr = vrnd
			co.vv = vval
		}

		co.rsvps++
		if co.rsvps >= co.cx.Quorum() {
			var v string

			if co.vr > 0 {
				v = co.vv
			} else {
				v = co.target
			}
			co.cval = v

			chosen := newNominate(co.crnd, v)
			co.outs.Put(chosen)
		}
	case tick:
		co.crnd += uint64(co.cx.Len())
		co.outs.Put(newInvite(co.crnd))
		co.vr = 0
		co.vv = ""
		co.rsvps = 0
		co.cval = ""
	}
}
