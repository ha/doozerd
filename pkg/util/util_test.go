package util

import (
	"borg/assert"
	"testing"
)

func TestPackui64(t *testing.T) {
	exp := []byte{0xab, 0xcd, 0x12, 0x34, 0x43, 0x21, 0xdc, 0xba}
	got := make([]byte, 8)
	Packui64(got, 0xabcd12344321dcba)
	assert.Equal(t, exp, got, "")
}
