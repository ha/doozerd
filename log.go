// Package log provides persistence for doozer using a circular log.
// Doozer mutations are apended to the log.  Deletions are garbage collected
// from the head of the log so the backing file does not grow indefinetly.

package log

import (
	"io"
	"os"
)

// Logger implements a journal using a file on disk. Doozer logs mutations
// to it to save state across server restarts.
type Logger struct {
	rws io.ReadWriteSeeker
}

// BUG(aram): The Logger is connected directly to the file, and deleted
// records are not garbage collected, the arena layer is not implemented yet.

// New opens the named log file for synchronous I/O, creating it with mode 0640
// if it does not exist and prepares it for logging operation.  If successful,
// methods on the returned logger can be used for I/O.  It returns a Logger
// and an error, if any.
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
