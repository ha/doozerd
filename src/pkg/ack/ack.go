package ack

import (
	"container/heap"
	"container/vector"
	"log"
	"math"
	"net"
	"os"
	"sync"
	"time"
)

const (
	interval = 1e8  // ns == 100ms
	timeout  = 1e10 // ns == 10s

	max = 3000 // bytes. Definitely big enough for UDP over Ethernet.
)

const (
	ack = 1 << iota
	// needAck // TODO add this
)


type packet struct {
	addr string
	flag byte
	data []byte
}


type Conn interface {
	ReadFrom([]byte) (int, net.Addr, os.Error)
	WriteTo([]byte, net.Addr) (int, os.Error)
}

type check struct {
	p         *packet
	at, until int64
}

func (k check) Less(y interface{}) bool {
	return k.at < y.(check).at
}

type Acker struct {
	c  Conn
	r  chan *packet
	wl sync.Mutex

	h    vector.Vector
	pend map[string]bool
	lk   sync.Mutex
}


func Ackify(c Conn) (a *Acker) {
	a = &Acker{
		c:    c,
		r:    make(chan *packet),
		pend: make(map[string]bool),
	}
	go a.recv()
	go a.time()
	return a
}


func (a *Acker) ReadFrom() (data []byte, addr string, err os.Error) {
	p := <-a.r
	if closed(a.r) {
		return nil, "", os.EINVAL
	}

	return p.data, p.addr, nil
}


// TODO add a "needAck bool" parameter
func (a *Acker) WriteTo(data []byte, addr string) {
	p := &packet{addr, 0, data}
	t := time.Nanoseconds()

	a.lk.Lock()
	a.pend[id(p)] = true
	heap.Push(&a.h, check{p, t + interval, t + timeout})
	a.lk.Unlock()

	err := a.write(p.flag, p.data, p.addr)
	if err != nil {
		log.Println(err)
	}
}


func (a *Acker) peek() check {
	if a.h.Len() < 1 {
		return check{at: math.MaxInt64}
	}

	return a.h.At(0).(check)
}


func (a *Acker) time() {
	ticker := time.NewTicker(interval / 4)
	defer ticker.Stop()

	for t := range ticker.C {
		a.lk.Lock()
		for k := a.peek(); t > k.at; k = a.peek() {
			heap.Pop(&a.h)
			i := id(k.p)

			if t > k.until {
				a.pend[i] = false, false
			}

			if a.pend[i] {
				heap.Push(&a.h, check{k.p, t + interval, k.until})

				a.lk.Unlock()
				log.Printf("udp-retry")
				err := a.write(k.p.flag, k.p.data, k.p.addr)
				if err != nil {
					log.Println(err)
				}
				a.lk.Lock()
			}
		}
		a.lk.Unlock()
	}
}


func (a *Acker) recv() {
	defer close(a.r)

	for {
		buf := make([]byte, max)
		n, addr, err := a.c.ReadFrom(buf)
		if err == os.EINVAL {
			return
		}
		if err != nil {
			log.Println(err)
			continue
		}

		p := &packet{addr.String(), buf[0], buf[1:n]}

		if p.flag&ack != 0 {
			a.lk.Lock()
			a.pend[id(p)] = false, false
			a.lk.Unlock()
			continue
		}

		a.r <- p

		// send ack
		err = a.write(p.flag|ack, p.data, p.addr)
		if err != nil {
			log.Println(err)
		}
	}
}


func (a *Acker) write(flag byte, data []byte, as string) os.Error {
	addr, err := net.ResolveUDPAddr(as)
	if err != nil {
		return err
	}

	buf := make([]byte, len(data)+1)
	buf[0] = flag
	copy(buf[1:], data)

	a.wl.Lock()
	_, err = a.c.WriteTo(buf, addr)
	a.wl.Unlock()

	return err
}


func id(p *packet) string {
	return p.addr + " " + string(p.data)
}
