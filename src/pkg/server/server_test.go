package server

import (
	"bytes"
	"doozer/store"
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
	c.del(&T{})
}
