package persistence

import (
	"os"
)

// arena translates a potentially fragmented file into a logically contiguous
// object.  It satisfies io.ReadWriter.
type arena struct {
	w *os.File // w and r point to the same file.
	r *os.File // w and r point to the same file.
}

// newArena opens the named file for synchronous I/O, creating it
// with mode 0640 if it does not exist and creates a translation layer.
// If successful, methods on the returned arena can be used for I/O.
// It returns an arena and an error, if any.  It is called by NewJournal.
func newArena(name string) (a *arena, err error) {
	// File is created if it does not exist, file must be opened synchronously
	// in order to guarantee consistency, file is group readable in order
	// to be read by an administrator if doozer is ran by its own user.
	a.w, err = os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0640)
	if err != nil {
		return nil, err
	}
	a.r, err = os.Open(name)
	if err != nil {
		return nil, err
	}
	return
}

// BUG(aram): The arena layer is not implemented yet. There is no
// offset translation performed yet, and deleted records are not
// garbage collected.

// Read reads up to len(b) bytes from the File after translating the
// logical arena offset into a physical file offset. It returns the
// number of bytes read and an error, if any. EOF is signaled by a
// zero count with err set to io.EOF.
func (a *arena) Read(p []byte) (n int, err error) {
	return a.r.Read(p)
}

// Write writes len(b) bytes to the File after translating the logical
// arena offset into a physical file offset. It returns the number of bytes
// written and an error, if any. Write returns a non-nil error
// when n != len(b).
func (a *arena) Write(p []byte) (n int, err error) {
	return a.w.Write(p)
}

// Close closes the backing Files, rendering the arena unusable for I/O.
// It returns an error, if any.
func (a *arena) Close() (err error) {
	err = a.r.Close()
	if err != nil {
		return
	}
	err = a.w.Close()
	return
}
