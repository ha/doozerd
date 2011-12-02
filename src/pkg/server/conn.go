package server

import (
	"doozer/consensus"
	"doozer/store"
	"encoding/binary"
	"goprotobuf.googlecode.com/hg/proto"
	"io"
	"log"
	"sync"
)

type conn struct {
	c        io.ReadWriter
	wl       sync.Mutex // write lock
	addr     string
	p        consensus.Proposer
	st       *store.Store
	canWrite bool
	rwsk     string
	rosk     string
	waccess  bool
	raccess  bool
}

func (c *conn) serve() {
	for {
		var t txn
		t.c = c
		err := c.read(&t.req)
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			return
		}
		t.run()
	}
}

func (c *conn) read(r *request) error {
	var size int32
	err := binary.Read(c.c, binary.BigEndian, &size)
	if err != nil {
		return err
	}

	buf := make([]byte, size)
	_, err = io.ReadFull(c.c, buf)
	if err != nil {
		return err
	}

	return proto.Unmarshal(buf, r)
}

func (c *conn) write(r *response) error {
	buf, err := proto.Marshal(r)
	if err != nil {
		return err
	}

	c.wl.Lock()
	defer c.wl.Unlock()

	err = binary.Write(c.c, binary.BigEndian, int32(len(buf)))
	if err != nil {
		return err
	}

	_, err = c.c.Write(buf)
	return err
}

// Grant compares sk against c.rwsk and c.rosk and
// updates c.waccess and c.raccess as necessary.
// It returns true if sk matched either password.
func (c *conn) grant(sk string) bool {
	switch sk {
	case c.rwsk:
		c.waccess = true
		c.raccess = true
		return true
	case c.rosk:
		c.raccess = true
		return true
	}
	return false
}
