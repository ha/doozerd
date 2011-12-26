// Package log implements a persistence layer for doozer using a circular log.
// Writes containing mutations are issued at the end of the log.  Deletions
// are garbage collected from the head of the log so that the backing file
// does not grow indefinetly.

package log

import (
	"io"
	"os"
)

type Logger struct {
	rws io.ReadWriteSeeker
}

// Function New opens the named log file for synchronous I/O, creating it with
// mode 0640 if it does not exist and prepares it for logging operation.
// If successful methods on the returned logger can be used for I/O.  It returns
// a Logger, and an error, if any.
//
// BUGS: The Logger is connected directly to the file, and deleted records are
// not garbage collected, the arena layer is not implemented yet.
func New(name string) (l *Logger, err error) {
	// File is created if it does not exist, file must be opened synchronously
	// in order to guarantee consistency, file is group readable in order
	// to be read by an administrator if doozer is ran by its own user.
	file, err := os.OpenFile(name, os.O_RDWR | os.O_CREATE | os.O_SYNC, 0640)
	if err != nil {
		return nil, err
	}
	l.rws = file
	return
}
