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

	sched bool
}

func (co *coordinator) deliver(p packet) (m *M, tick bool) {
	in := &p.M

	if in.Cmd == nil {
		return
	}

	switch *in.Cmd {
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
		return &M{Cmd: invite, Crnd: &co.crnd}, tick
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

		if i > co.crnd && !co.sched {
			co.sched = true
			tick = true
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

			return &M{Cmd: nominate, Crnd: &co.crnd, Value: []byte(v)}, tick
		}
	case M_TICK:
		co.crnd += int64(co.size)
		co.vr = 0
		co.vv = ""
		co.rsvps = make(map[string]bool)
		co.cval = ""
		co.sched = false
		return &M{Cmd: invite, Crnd: &co.crnd}, tick
	}

	return
}
