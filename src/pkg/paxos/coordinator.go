package paxos

type coordinator struct {
	cx   *cluster
	outs Putter

	begun  bool
	target string
	crnd   int64
	cval   string
	rsvps  map[int]bool
	vr     int64
	vv     string

	seen int64
}

func (co *coordinator) Put(in *M) {
	if co.crnd == 0 {
		co.crnd += int64(co.cx.Len())
	}

	switch in.Cmd() {
	case M_PROPOSE:
		if co.begun {
			break
		}

		co.begun = true
		co.target = string(in.Value)
		co.outs.Put(&M{WireCmd: invite, Crnd: &co.crnd})
		co.vr = 0
		co.vv = ""
		co.rsvps = make(map[int]bool)
		co.cval = ""
	case M_RSVP:
		if !co.begun {
			break
		}

		i, vrnd, vval := *in.Crnd, *in.Vrnd, in.Value

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
			co.vv = string(vval)
		}

		co.rsvps[in.From()] = true
		if len(co.rsvps) >= co.cx.Quorum() {
			var v string

			if co.vr > 0 {
				v = co.vv
			} else {
				v = co.target
			}
			co.cval = v

			co.outs.Put(&M{WireCmd: nominate, Crnd: &co.crnd, Value: []byte(v)})
		}
	case M_TICK:
		co.crnd += int64(co.cx.Len())
		co.outs.Put(&M{WireCmd: invite, Crnd: &co.crnd})
		co.vr = 0
		co.vv = ""
		co.rsvps = make(map[int]bool)
		co.cval = ""
	}
}
