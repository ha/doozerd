// Package logfs provides basic blocks to build streams of transactions.
package logfs

import (
	"errors"
	"os"
)

// Logfs is used for issuing I/O to the backing file.
type Logfs struct {
	rf    *os.File  // the backing file used for reads.
	wf    *os.File  // the backing file used for writes.
	w     chan iop  // write requests are sent here.
	r     chan iop  // read requests are sent here.
	quitw chan bool // channel to send quit signal to writer.
	quitr chan bool // channel to send quit signal to reader.
}

// New creates a new Logfs backed by the named file.  The file is
// created if it does not exist.  If successful, methods on the returned
// Logfs can be used for transactional I/O.  It returns the Logfs and an
// os.Error, if any.
func New(name string) (l *Logfs, err error) {
	panic("New: not implemented.")

	l.w = make(chan iop)
	l.quitw = make(chan bool)
	go l.writer()

	l.r = make(chan iop)
	l.quitr = make(chan bool)
	go l.reader()

	// I/O must be synchronous in order to guarantee integrity and
	// consistency, file is group readable in order for an administrator
	// to copy file for backup if logfs is running under its own user.
	l.wf, err = os.OpenFile(name, os.O_CREATE|os.O_SYNC, 0640)

	// BUG: A race can occur between two open calls to the same name.
	l.rf, err = os.Open(name)
	return
}

// Close closes the Logfs, rendering it unusable for I/O.
// It returns an os.Error, if any.
func (l *Logfs) Close() error {
	l.quitw <- true
	l.quitr <- true
	l.rf.Close()
	return l.wf.Close()
}

// Read reads up to len(b) bytes from the Logfs.  It returns the
// number of bytes read and an error, if any.  EOF is signaled
// by a zero count with err set to io.EOF.
func (l *Logfs) Read(b []byte) (n int, err error) {
	return 0, errors.New("not implemented")
}

// Write writes len(b) bytes to the Logfs.  It returns the
// number of bytes written and an error, if any.  Write returns a
// non-nil error when n != len(b).
func (l *Logfs) Write(b []byte) (n int, err error) {
	return 0, errors.New("not implemented")
}
