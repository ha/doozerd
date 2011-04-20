package peer


type liveness struct {
	timeout int64
	prev    int64
	ival    int64
	times   map[string]int64
	self    string
	shun    chan<- string
}


func (lv *liveness) check(t int64) {
	if t > lv.prev+lv.ival {
		n := t - lv.timeout
		for addr, s := range lv.times {
			if n > s && addr != lv.self {
				lv.times[addr] = 0, false
				lv.shun <- addr
			}
		}
		lv.prev = t
	}
}
