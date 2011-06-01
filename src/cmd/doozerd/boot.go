package main

import (
	"crypto/rand"
	"encoding/base32"
	"github.com/ha/doozer"
	"os"
	"time"
)

const attachTimeout = 1e9

func boot(name, id, laddr, buri string) *doozer.Conn {
	b, err := doozer.DialUri(buri, "")
	if err != nil {
		panic(err)
	}

	err = b.Access(rwsk)
	if err != nil {
		panic(err)
	}

	cl := lookupAndAttach(b, name)
	if cl == nil {
		return elect(name, id, laddr, b)
	}

	return cl
}


// Elect chooses a seed node, and returns a connection to a cal.
// If this process is the seed, returns nil.
func elect(name, id, laddr string, b *doozer.Conn) *doozer.Conn {
	// advertise our presence, since we might become a cal
	nspath := "/ctl/ns/" + name + "/" + id
	r, err := b.Set(nspath, 0, []byte(laddr))
	if err != nil {
		panic(err)
	}

	// fight to be the seed
	_, err = b.Set("/ctl/boot/"+name, 0, []byte(id))
	if err, ok := err.(*doozer.Error); ok && err.Err == doozer.ErrOldRev {
		// we lost, lookup addresses again
		cl := lookupAndAttach(b, name)
		if cl == nil {
			panic("failed to attach after losing election")
		}

		// also delete our entry, since we're not officially a cal yet.
		// it gets set again in peer.Main when we become a cal.
		err := b.Del(nspath, r)
		if err != nil {
			panic(err)
		}

		return cl
	} else if err != nil {
		panic(err)
	}

	return nil // we are the seed node -- don't attach
}


func lookupAndAttach(b *doozer.Conn, name string) *doozer.Conn {
	as := lookup(b, name)
	if len(as) > 0 {
		cl := attach(name, as)
		if cl != nil {
			return cl
		}
	}
	return nil
}


func attach(name string, addrs []string) *doozer.Conn {
	ch := make(chan *doozer.Conn, 1)

	for _, a := range addrs {
		go func(a string) {
			if c, _ := isCal(name, a); c != nil {
				ch <- c
			}
		}(a)
	}

	go func() {
		<-time.After(attachTimeout)
		ch <- nil
	}()

	return <-ch
}


// IsCal checks if addr is a CAL in the cluster named name.
// Returns a client if so, nil if not.
func isCal(name, addr string) (*doozer.Conn, os.Error) {
	c, err := doozer.Dial(addr)
	if err != nil {
		return nil, err
	}

	err = c.Access(rwsk)
	if err != nil {
		return nil, err
	}

	v, _, _ := c.Get("/ctl/name", nil)
	if string(v) != name {
		return nil, nil
	}

	rev, err := c.Rev()
	if err != nil {
		return nil, err
	}

	var cals []string
	names, err := c.Getdir("/ctl/cal", rev, 0, -1)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		cals = append(cals, name)
	}

	for _, cal := range cals {
		body, _, err := c.Get("/ctl/cal/"+cal, nil)
		if err != nil || len(body) == 0 {
			continue
		}

		id := string(body)

		v, _, err := c.Get("/ctl/node/"+id+"/addr", nil)
		if err != nil {
			return nil, err
		}
		if string(v) == addr {
			return c, nil
		}
	}

	return nil, nil
}


// Find possible addresses for cluster named name.
func lookup(b *doozer.Conn, name string) (as []string) {
	rev, err := b.Rev()
	if err != nil {
		panic(err)
	}

	path := "/ctl/ns/" + name
	names, err := b.Getdir(path, rev, 0, -1)
	if err, ok := err.(*doozer.Error); ok && err.Err == doozer.ErrNoEnt {
		return nil
	} else if err != nil {
		panic(err)
	}

	path += "/"
	for _, name := range names {
		body, _, err := b.Get(path+name, &rev)
		if err != nil {
			panic(err)
		}
		as = append(as, string(body))
	}
	return as
}


func randId() string {
	const bits = 80 // enough for 10**8 ids with p(collision) < 10**-8
	rnd := make([]byte, bits/8)

	n, err := rand.Read(rnd)
	if err != nil {
		panic(err)
	}
	if n != len(rnd) {
		panic("io.ReadFull len mismatch")
	}

	enc := make([]byte, base32.StdEncoding.EncodedLen(len(rnd)))
	base32.StdEncoding.Encode(enc, rnd)
	return string(enc)
}
