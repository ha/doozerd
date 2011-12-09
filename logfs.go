// Package logfs provides basic blocks to build streams of transactions.
package logfs

import "os"

// A Record is the fundamental unit you read, write or delete.
type Record struct {
	Data []byte // data to set or retrieve.
	Seqn uint64 // Sequence number, each I/O operation has one.
}

// Logfs is used for issuing I/O to the backing file.
type Logfs struct {
	file *os.File // the backing file
}

// NewLogfs creates a new Logfs backed by the named file.  The file is
// created if it does not exist.  If successful, methods on the returned
// Logfs can be used for transactional I/O.  It returns the Logfs and an
// os.Error, if any.
func NewLogfs(name string) (l *Logfs, err os.Error) {
	l.file, err = os.OpenFile(name, os.O_CREATE | os.O_SYNC, 0640)
	return
}

// CloseLogfs closes the Logfs, rendering it unusable for I/O.
// It returns an os.Error, if any.
func (l *Logfs) CloseLogfs() os.Error {
	return l.file.Close()
}

// Read reads the next record from dis. Once a record had been read,
// it will never be read again. Read returns an os.Error if some error
// occurred.
func (l *Logfs) Read() (r Record, err os.Error) {
	return Record{}, os.NewError("not implemented")
}

// Write writes the record and returns after the record was commited
// to disk.  It returns an os.Error if some error occurred.
func (l *Logfs) Write(r Record) os.Error {
	return os.NewError("not implemented")
}
