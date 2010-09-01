package util

import (
	"junta/assert"
	"testing"
)

func TestPackui64(t *testing.T) {
	exp := []byte{0xab, 0xcd, 0x12, 0x34, 0x43, 0x21, 0xdc, 0xba}
	got := make([]byte, 8)
	Packui64(got, 0xabcd12344321dcba)
	assert.Equal(t, exp, got)
}

func TestUnpackui64(t *testing.T) {
	b := []byte{0xab, 0xcd, 0x12, 0x34, 0x43, 0x21, 0xdc, 0xba}
	got := Unpackui64(b)
	assert.Equal(t, uint64(0xabcd12344321dcba), got)
}

func TestRandString(t *testing.T) {
	got := RandString(160)
	assert.Equal(t, 40, len(got))
	// Is Hex?
	for _, x := range got {
		assert.T(t, (x >= 'a' && x <= 'f') || (x >= '0' && x <= '9'))
	}
}
