package exec

import (
	"syscall"
)

func dup2(from, to int) (errno int) {
	return syscall.Dup2(from, to)
}
