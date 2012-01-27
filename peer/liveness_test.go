package peer

import (
	"github.com/bmizerany/assert"
	"net"
	"testing"
)

func TestLivenessMark(t *testing.T) {
	a1, err := net.ResolveUDPAddr("udp", "127.0.0.1:8046")
	if err != nil {
		panic(err)
	}
	a2, err := net.ResolveUDPAddr("udp", "127.0.0.2:8046")
	if err != nil {
		panic(err)
	}
	lv := liveness{
		timeout: 10,
		ival:    5,
		self:    a1,
		shun:    make(chan string, 100),
	}

	lv.mark(a1, 1)
	assert.Equal(t, []liverec{{a1, 1}}, lv.times)
	lv.mark(a2, 2)
	assert.Equal(t, []liverec{{a1, 1}, {a2, 2}}, lv.times)
}

func TestLivenessStaysAlive(t *testing.T) {
	shun := make(chan string, 1)
	a := &net.UDPAddr{net.IP{1, 2, 3, 4}, 5}
	lv := liveness{
		prev:    0,
		ival:    1,
		timeout: 3,
		times:   []liverec{{a, 5}},
		shun:    shun,
	}
	lv.check(7)
	assert.Equal(t, int64(7), lv.prev)
	assert.Equal(t, 0, len(shun))
	assert.Equal(t, []liverec{{a, 5}}, lv.times)
}

func TestLivenessTimesOut(t *testing.T) {
	shun := make(chan string, 1)
	a := &net.UDPAddr{net.IP{1, 2, 3, 4}, 5}
	lv := liveness{
		prev:    0,
		ival:    1,
		timeout: 3,
		times:   []liverec{{a, 5}},
		shun:    shun,
		self:    &net.UDPAddr{net.IP{2, 3, 4, 5}, 6},
	}
	lv.check(9)
	assert.Equal(t, int64(9), lv.prev)
	assert.Equal(t, 1, len(shun))
	assert.Equal(t, "1.2.3.4:5", <-shun)
	assert.Equal(t, []liverec{}, lv.times)
}

func TestLivenessSelfStaysAlive(t *testing.T) {
	shun := make(chan string, 1)
	a := &net.UDPAddr{net.IP{1, 2, 3, 4}, 5}
	lv := liveness{
		prev:    0,
		ival:    1,
		timeout: 3,
		times:   []liverec{{a, 5}},
		shun:    shun,
		self:    a,
	}
	lv.check(9)
	assert.Equal(t, int64(9), lv.prev)
	assert.Equal(t, 0, len(shun))
	assert.Equal(t, []liverec{{a, 5}}, lv.times)
}

func TestLivenessNoCheck(t *testing.T) {
	a := &net.UDPAddr{net.IP{1, 2, 3, 4}, 5}
	lv := liveness{
		prev:  5,
		ival:  3,
		times: []liverec{{a, 5}},
	}
	lv.check(7)
	assert.Equal(t, int64(5), lv.prev)
	assert.Equal(t, []liverec{{a, 5}}, lv.times)
}
