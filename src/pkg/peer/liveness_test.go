package peer

import (
	"github.com/bmizerany/assert"
	"testing"
)


func TestLivenessStaysAlive(t *testing.T) {
	shun := make(chan string, 1)
	lv := liveness{
		prev:    0,
		ival:    1,
		timeout: 3,
		times:   map[string]int64{"a": 5},
		shun:    shun,
	}
	lv.check(7)
	assert.Equal(t, int64(7), lv.prev)
	assert.Equal(t, 0, len(shun))
	assert.Equal(t, map[string]int64{"a": 5}, lv.times)
}


func TestLivenessTimesOut(t *testing.T) {
	shun := make(chan string, 1)
	lv := liveness{
		prev:    0,
		ival:    1,
		timeout: 3,
		times:   map[string]int64{"a": 5},
		shun:    shun,
	}
	lv.check(9)
	assert.Equal(t, int64(9), lv.prev)
	assert.Equal(t, 1, len(shun))
	assert.Equal(t, "a", <-shun)
	assert.Equal(t, map[string]int64{}, lv.times)
}


func TestLivenessSelfStaysAlive(t *testing.T) {
	shun := make(chan string, 1)
	lv := liveness{
		prev:    0,
		ival:    1,
		timeout: 3,
		times:   map[string]int64{"a": 5},
		shun:    shun,
		self:    "a",
	}
	lv.check(9)
	assert.Equal(t, int64(9), lv.prev)
	assert.Equal(t, 0, len(shun))
	assert.Equal(t, map[string]int64{"a": 5}, lv.times)
}


func TestLivenessNoCheck(t *testing.T) {
	lv := liveness{
		prev:  5,
		ival:  3,
		times: map[string]int64{"a": 5},
	}
	lv.check(7)
	assert.Equal(t, int64(5), lv.prev)
	assert.Equal(t, map[string]int64{"a": 5}, lv.times)
}
