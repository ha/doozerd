package paxos

func coordinator(ins chan Msg, cx *cluster, outs Putter) {
	crnd := uint64(cx.SelfIndex())
	if crnd == 0 {
		crnd += uint64(cx.Len())
	}

	var target string
	var cval string
	var rsvps int
	var vr uint64
	var vv string

	// Wait for the very first proposal
	for in := range ins {
		if in.Cmd() != propose {
			continue
		}
		target = proposeParts(in)
		outs.Put(newInvite(crnd))
		vr = 0
		vv = ""
		rsvps = 0
		cval = ""
		break
	}

	for in := range ins {
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
			if rsvps >= cx.Quorum() {
				var v string

				if vr > 0 {
					v = vv
				} else {
					v = target
				}
				cval = v

				chosen := newNominate(crnd, v)
				outs.Put(chosen)
			}
		case tick:
			crnd += uint64(cx.Len())
			outs.Put(newInvite(crnd))
			vr = 0
			vv = ""
			rsvps = 0
			cval = ""
		}
	}
}
