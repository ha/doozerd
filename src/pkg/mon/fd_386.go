package mon

import (
	"syscall"
)

const nfdbits = 32

func fdDelt(n int) int {
	return n / nfdbits
}

func fdMask(n int) int32 {
	return 1 << uint(n%nfdbits)
}

func fdAdd(s *syscall.FdSet, n int) {
	s.Bits[fdDelt(n)] |= fdMask(n)
}

func fdIsSet(s *syscall.FdSet, n int) bool {
	return (s.Bits[fdDelt(n)] & fdMask(n)) != 0
}
