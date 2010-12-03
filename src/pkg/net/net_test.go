package net

import (
	"doozer/paxos"
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
func (fd FakeConn) LocalAddr() (addr net.Addr) {
	return
}

func TestNetClose(t *testing.T) {
	r := Ackify(FakeConn(0), make(chan paxos.Packet))
	<-r
	assert.T(t, closed(r))
}
