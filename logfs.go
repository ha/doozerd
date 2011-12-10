// Package logfs provides basic blocks to build streams of transactions.
package logfs

import (
	"errors"
	"os"
)

// A Record is the fundamental unit you read, write or delete.
type Record struct {
	Data []byte // data to set or retrieve.
	Seqn uint64 // Sequence number, each I/O operation has one.
}

// Logfs is used for issuing I/O to the backing file.
type Logfs struct {
	file  *os.File  // the backing file.
	w     chan iop  // writes are issued here.
	quitw chan bool // channel to send quit signal to writer.
}

// New creates a new Logfs backed by the named file.  The file is
// created if it does not exist.  If successful, methods on the returned
// Logfs can be used for transactional I/O.  It returns the Logfs and an
// os.Error, if any.
func New(name string) (l *Logfs, err error) {
	panic("not implemented")

	l.w = make(chan iop)
	l.quitw = make(chan bool)
	go l.writer()

	// I/O must be synchronous in order to guarantee integrity and
	// consistency, file is group readable in order for an administrator
	// to copy file for backup is logfs is running under its own user.
	l.file, err = os.OpenFile(name, os.O_CREATE|os.O_SYNC, 0640)
	return
}

// Close closes the Logfs, rendering it unusable for I/O.
// It returns an os.Error, if any.
func (l *Logfs) Close() error {
	l.quitw <- true
	return l.file.Close()
}

// Read reads the next record from disk. Once a record had been read,
// it will never be read again. Read returns the record read and an
// os.Error if some error occurred.
func (l *Logfs) Read() (r Record, err error) {
	return Record{}, errors.New("not implemented")
}

// Write writes the record and returns after the record was commited
// to disk.  It returns an os.Error if some error occurred.
func (l *Logfs) Write(r Record) error {
	return errors.New("not implemented")
}
