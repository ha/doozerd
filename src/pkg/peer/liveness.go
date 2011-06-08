package peer

import (
	"net"
	"log"
)


type liverec struct {
	addr *net.UDPAddr
	seen int64
}


type liveness struct {
	timeout int64
	prev    int64
	ival    int64
	times   []liverec
	self    *net.UDPAddr
	shun    chan<- string
}


func (lv *liveness) mark(a net.Addr, t int64) {
	uaddr, ok := a.(*net.UDPAddr)
	if !ok {
		return
	}
	var i int
	for i := 0; i < len(lv.times); i++ {
		if eq(lv.times[i].addr, uaddr) {
			lv.times[i].seen = t
			break
		}
	}
	if i == len(lv.times) {
		lv.times = append(lv.times, liverec{uaddr, t})
	}
}


func (lv *liveness) check(t int64) {
	if t > lv.prev+lv.ival {
		n := t - lv.timeout
		times := make([]liverec, len(lv.times))
		var i int
		for _, r := range lv.times {
			if n < r.seen || eq(r.addr, lv.self) {
				times[i] = r
				i++
			} else {
				log.Printf("shunning addr=%s", r.addr)
				lv.shun <- r.addr.String()
			}
		}
		lv.times = times[:i]
		lv.prev = t
	}
}


func eq(a, b *net.UDPAddr) bool {
	return a.Port == b.Port && a.IP.Equal(b.IP)
}
