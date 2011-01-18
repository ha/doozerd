package ack

import (
	"github.com/bmizerany/assert"
	"net"
	"os"
	"testing"
)

type FakeConn int

func (fd FakeConn) ReadFrom([]byte) (int, net.Addr, os.Error) {
	var a net.Addr
	return 0, a, os.EINVAL
}
func (fd FakeConn) WriteTo([]byte, net.Addr) (int, os.Error) {
	return 0, os.EINVAL
}

func TestNetClose(t *testing.T) {
	a := Ackify(FakeConn(0))
	_, _, err := a.ReadFrom()
	assert.T(t, closed(a.r))
	assert.Equal(t, os.EINVAL, err)
}
