package server

import (
	"bytes"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"testing"
)


func mustUnmarshal(b []byte) (r *R) {
	r = new(R)
	err := proto.Unmarshal(b, r)
	if err != nil {
		panic(err)
	}
	return
}


func assertResponse(t *testing.T, exp *R, c *conn) {
	b := c.c.(*bytes.Buffer).Bytes()
	assert.T(t, len(b) > 4, b)
	assert.Equal(t, exp, mustUnmarshal(b[4:]))
}


func TestDelNilFields(t *testing.T) {
	c := &conn{
		c:   &bytes.Buffer{},
		s:   &Server{},
		cal: true,
		tx:  make(map[int32]txn),
	}
	c.del(&T{Tag: proto.Int32(1)}, newTxn())
	assertResponse(t, missingArg, c)
}


func TestSetNilFields(t *testing.T) {
	c := &conn{
		c:   &bytes.Buffer{},
		s:   &Server{},
		cal: true,
		tx:  make(map[int32]txn),
	}
	c.set(&T{Tag: proto.Int32(1)}, newTxn())
	assertResponse(t, missingArg, c)
}

func TestServerCloseTxn(t *testing.T) {
	c := &conn{
		tx: make(map[int32]txn),
	}

	tx := newTxn()
	c.tx[1] = tx

	c.closeTxn(1)

	assert.Equal(t, map[int32]txn{}, c.tx)
	assert.Equal(t, false, <-tx.done)
}
