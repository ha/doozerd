package server

import (
	"doozer/consensus"
	"doozer/store"
	"encoding/binary"
	"goprotobuf.googlecode.com/hg/proto"
	"io"
	"log"
	"os"
	"sync"
)


type conn struct {
	c        io.ReadWriter
	wl       sync.Mutex // write lock
	addr     string
	p        consensus.Proposer
	st       *store.Store
	canWrite bool
	secret   string
	access   bool
}


func (c *conn) serve() {
	for {
		var t txn
		t.c = c
		err := c.read(&t.req)
		if err != nil {
			if err != os.EOF {
				log.Println(err)
			}
			return
		}
		t.run()
	}
}


func (c *conn) read(r *request) os.Error {
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


func (c *conn) write(r *response) os.Error {
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
