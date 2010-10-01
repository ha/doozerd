package mon

import (
	"syscall"
)

func dup2(from, to int) (errno int) {
	_, errno = syscall.Dup2(from, to)
	return
}
