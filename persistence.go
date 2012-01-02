// Package persistence allows doozer to save state across server restarts.
// Doozer mutations are apended to the file on disk.  Deletions are
// garbage collected from the head of the log file so it does not
// grow indefinitely.

package persistence

import (
	"io"
)

// Journal represents a file where doozer can save state.
type Journal struct {
	rw io.ReadWriter // arena
}

type iop struct {
	mut string
	c   chan error
}

// NewJournal opens the named file for synchronous I/O, creating it
// with mode 0640 if it does not exist and prepares it for logging operation.
// If successful, methods on the returned Journal can be used for I/O.
// It returns a Journal and an error, if any.
func NewJournal(name string) (j *Journal, err error) {
	j.rw, err = newArena(name)
	if err != nil {
		return nil, err
	}
	return
}

// Store writes the mutation to the Journal.
func (j *Journal) Store(mutation string) error {
	panic("not implemented")
	return nil
}

// Retrieve reads the next mutation from the Journal.  It returns
// the mutation and an error, if any.  EOF is signaled by a nil
// mutation with err set to io.EOF
func (j *Journal) Retrieve() (mutation string, err error) {
	panic("not implemented")
	return "", nil
}
