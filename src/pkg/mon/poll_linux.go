package mon

import (
	"syscall"
)

func selectFds(n int, r, w, e *syscall.FdSet, t *syscall.Timeval) (errno int) {
	_, errno = syscall.Select(n, r, w, e, t)
	return
}
