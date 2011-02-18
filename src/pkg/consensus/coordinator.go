package consensus


type coordinator struct {
	size int
	quor int

	begun  bool
	target string
	crnd   int64
	cval   string
	rsvps  map[string]bool
	vr     int64
	vv     string

	seen int64
}

func (co *coordinator) Deliver(p packet) (m *M) {
	in := &p.M

	if co.crnd == 0 {
		co.crnd += int64(co.size)
	}

	if in.WireCmd == nil {
		return nil
	}

	switch *in.WireCmd {
	case M_PROPOSE:
		if co.begun {
			break
		}

		co.begun = true
		co.target = string(in.Value)
		co.vr = 0
		co.vv = ""
		co.rsvps = make(map[string]bool)
		co.cval = ""
		return &M{WireCmd: invite, Crnd: &co.crnd}
	case M_RSVP:
		if !co.begun {
			break
		}

		if in.Crnd == nil || in.Vrnd == nil {
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

		co.rsvps[p.Addr] = true
		if len(co.rsvps) >= co.quor {
			var v string

			if co.vr > 0 {
				v = co.vv
			} else {
				v = co.target
			}
			co.cval = v

			return &M{WireCmd: nominate, Crnd: &co.crnd, Value: []byte(v)}
		}
	case M_TICK:
		co.crnd += int64(co.size)
		co.vr = 0
		co.vv = ""
		co.rsvps = make(map[string]bool)
		co.cval = ""
		return &M{WireCmd: invite, Crnd: &co.crnd}
	}

	return
}
