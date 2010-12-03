package mon

import (
	"syscall"
)

const nfdbits = 64

func fdDelt(n int) int {
	return n / nfdbits
}

func fdMask(n int) int64 {
	return 1 << uint(n%nfdbits)
}

func fdAdd(s *syscall.FdSet, n int) {
	s.Bits[fdDelt(n)] |= fdMask(n)
}

func fdIsSet(s *syscall.FdSet, n int) bool {
	return (s.Bits[fdDelt(n)] & fdMask(n)) != 0
}
