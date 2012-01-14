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
	r     io.Reader  // mutations are read from here.
	w     io.Writer  // mutations are written here.
}

// NewJournal opens the named file for synchronous I/O, creating it
// with mode 0640 if it does not exist and prepares it for logging operation.
// If successful, methods on the returned Journal can be used for I/O.
// It returns a Journal and an error, if any.
func NewJournal(name string) (j *Journal, err error) {
	// File is created if it does not exist, file must be opened synchronously
	// in order to guarantee consistency, file is group readable in order
	// to be read by an administrator if doozer is ran by its own user.
	w, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0640)
	if err != nil {
		return
	}
	r, err := os.Open(name) // same file as w.
	if err != nil {
		return
	}

	j = &Journal{r: r, w: w}
	return
}

// Store writes the mutation to the Journal.
func (j Journal) WriteMutation(m string) error {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	return writeMutation(j.w, m)
}

// Retrieve reads the next mutation from the Journal.  It returns
// the mutation and an error, if any.  EOF is signaled by a nil
// mutation with err set to io.EOF
func (j Journal) ReadMutation() (m string, err error) {
	j.mutex.Lock()
	defer j.mutex.Unlock()
	return readMutation(j.r)
}

// Close shuts down the journal.
func (j Journal) Close() {
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
func writeMutation(w io.Writer, mut string) (err error) {
	b := newBlock(mut)

	// We use two write calls bacause we use encoding/binary
	// to write the fixed length header.
	err = binary.Write(w, binary.LittleEndian, b.Hdr)
	if err != nil {
		return
	}

	// We'we written the header successfully, write the rest
	// of the data.
	_, err = w.Write(b.Data)
	return
}
