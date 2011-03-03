package server

import (
	"bytes"
	"doozer/store"
	"doozer/util"
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
		c:     &bytes.Buffer{},
		s:     &Server{},
		cal:   true,
		snaps: make(map[int32]store.Getter),
		tx:    make(map[int32]txn),
		log:   util.NewLogger("test"),
	}
	c.del(&T{Tag: proto.Int32(1)}, newTxn())
	assertResponse(t, missingArg, c)
}


func TestDelSnapNilFields(t *testing.T) {
	c := &conn{
		c:     &bytes.Buffer{},
		s:     &Server{},
		cal:   true,
		snaps: make(map[int32]store.Getter),
		tx:    make(map[int32]txn),
		log:   util.NewLogger("test"),
	}
	c.delSnap(&T{Tag: proto.Int32(1)}, newTxn())
	assertResponse(t, missingArg, c)
}


func TestCheckinNilFields(t *testing.T) {
	c := &conn{
		c:     &bytes.Buffer{},
		s:     &Server{},
		cal:   true,
		snaps: make(map[int32]store.Getter),
		tx:    make(map[int32]txn),
		log:   util.NewLogger("test"),
	}
	c.checkin(&T{Tag: proto.Int32(1)}, newTxn())
	assertResponse(t, missingArg, c)
}


func TestSetNilFields(t *testing.T) {
	c := &conn{
		c:     &bytes.Buffer{},
		s:     &Server{},
		cal:   true,
		snaps: make(map[int32]store.Getter),
		tx:    make(map[int32]txn),
		log:   util.NewLogger("test"),
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


func TestServerCancel(t *testing.T) {
	var buf bytes.Buffer
	c := &conn{
		c:     &buf,
		s:     &Server{},
		cal:   true,
		snaps: make(map[int32]store.Getter),
		tx:    make(map[int32]txn),
		log:   util.NewLogger("test"),
	}

	fakeTx := newTxn()
	c.tx[1] = fakeTx

	cancelTx := newTxn()
	c.tx[2] = cancelTx

	done := make(chan bool, 1)
	go func() {
		c.cancel(&T{Tag: proto.Int32(2), Id: proto.Int32(1)}, cancelTx)
		done <- true
	}()

	// fake verb
	<-fakeTx.cancel
	c.closeTxn(1) // standard thing to do for every verb

	<-done
	b := buf.Bytes()
	assert.NotEqual(t, []byte{}, b)
	assert.Equal(t, []byte{0, 0, 0, 4}, b[0:4])

	exp := &R{
		Tag:   proto.Int32(2),
		Flags: proto.Int32(Valid | Done),
	}
	assert.Equal(t, exp, mustUnmarshal(b[4:]))
}
