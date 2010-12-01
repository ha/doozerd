package doozer

import (
	"doozer/client"
	"github.com/bmizerany/assert"
	"net"
	"testing"
)

func mustListen() net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	return l
}

func TestDoozerSimple(t *testing.T) {
	l := mustListen()
	defer l.Close()

	go Main("a", "", l, nil)

	cl, err := client.Dial(l.Addr().String())
	assert.Equal(t, nil, err)
	assert.Equal(t, nil, cl.Noop())
}
