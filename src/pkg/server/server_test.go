package server

import (
	"bytes"
	"doozer/store"
	"github.com/bmizerany/assert"
	"testing"
)

func TestDelNilFields(t *testing.T) {
	c := &conn{
		c:       &bytes.Buffer{},
		s:       &Server{},
		cal:     true,
		snaps:   make(map[int32]store.Getter),
		cancels: make(map[int32]chan bool),
	}
	r := c.del(&T{})
	assert.Equal(t, missingArg, r)
}


func TestDelSnapNilFields(t *testing.T) {
	c := &conn{
		c:       &bytes.Buffer{},
		s:       &Server{},
		cal:     true,
		snaps:   make(map[int32]store.Getter),
		cancels: make(map[int32]chan bool),
	}
	r := c.delSnap(&T{})
	assert.Equal(t, missingArg, r)
}


func TestCheckinNilFields(t *testing.T) {
	c := &conn{
		c:       &bytes.Buffer{},
		s:       &Server{},
		cal:     true,
		snaps:   make(map[int32]store.Getter),
		cancels: make(map[int32]chan bool),
	}
	r := c.checkin(&T{})
	assert.Equal(t, missingArg, r)
}


func TestSetNilFields(t *testing.T) {
	c := &conn{
		c:       &bytes.Buffer{},
		s:       &Server{},
		cal:     true,
		snaps:   make(map[int32]store.Getter),
		cancels: make(map[int32]chan bool),
	}
	r := c.set(&T{})
	assert.Equal(t, missingArg, r)
}

func TestServerCloseTxn(t *testing.T) {
	c := &conn{
		tx: make(map[int32]txn),
	}

	tx := newTxn()
	c.tx[1] = tx

	c.closeTxn(1, tx)

	assert.Equal(t, map[int32]txn{}, c.tx)
	assert.Equal(t, false, <-tx.done)
}
