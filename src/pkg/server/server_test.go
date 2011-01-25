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
