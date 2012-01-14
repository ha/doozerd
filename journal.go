// Package persistence allows doozer to save state across server restarts.
// Doozer mutations are apended to a file on disk.  When a file reaches
// a maximum size a new file is used.  Old files are sometimes
// rewritten to prune deleted mutation.

package persistence

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

// Journal represents a file where doozer can save state.  Doozer usally
// uses a list of multiple journals.
type Journal struct {
	mutex sync.Mutex // each mutation must be read/written atomically.
	rw    *os.File   // mutations are read/written from here.
}

// NewJournal opens the named file for synchronous I/O, creating it
// with mode 0640 if it does not exist and prepares it for logging operation.
// If successful, methods on the returned Journal can be used for I/O.
// It returns a Journal and an error, if any.
func NewJournal(name string) (j *Journal, err error) {
	// File is created if it does not exist, file must be opened synchronously
	// in order to guarantee consistency, file is group readable in order
	// to be read by an administrator if doozer is ran by its own user, file
	// is append-only because data is never overwritten.
	f, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_RDWR|os.O_SYNC, 0640)
	if err != nil {
		return
	}

	j = &Journal{rw: f}
	return
}

// Store writes the mutation to the Journal.
func (j Journal) WriteMutation(m string) (err error) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	return writeMutation(j.rw, m)
}

// Retrieve reads the next mutation from the Journal.  It returns
// the mutation and an error, if any.  EOF is signaled by a nil
// mutation with err set to io.EOF
func (j Journal) ReadMutation() (m string, err error) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	return readMutation(j.rw)
}

// Close shuts down the journal.
func (j Journal) Close() {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.rw.Close()
}

// readMutation reads a block from the reader, decodes it into a
// mutation and returns it along with an error.
func readMutation(r io.Reader) (mut string, err error) {
	b := block{}

	// Read the header so we know how much to read next.
	err = binary.Read(r, binary.LittleEndian, &b.Hdr)
	if err != nil {
		return
	}

	// If everything went fine, we can read the data.
	b.Data = make([]byte, int(b.Hdr.Size))
	_, err = io.ReadFull(r, b.Data)
	if err != nil {
		return
	}

	// We need to make sure the checksum is valid.
	if b.isValid() != true {
		err = errors.New("checksum check failed")
		return
	}

	mut = string(b.Data)
	return
}

// writeMutation encodes a mutation into a block and writes the
// block to the writer returning an error.
func writeMutation(w io.WriteSeeker, mut string) (err error) {
	b := newBlock(mut)

	offset, _ := w.Seek(0, 1)
	// We use two write calls bacause we use encoding/binary
	// to write the fixed length header.
	err = binary.Write(w, binary.LittleEndian, b.Hdr)
	if err != nil {
		return
	}

	// We'we written the header successfully, write the rest
	// of the data.
	_, err = w.Write(b.Data)
	w.Seek(offset, 0)
	return
}
