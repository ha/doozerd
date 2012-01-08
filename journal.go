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
)

// Journal represents a file where doozer can save state.  Doozer usally
// uses a list of multiple journals.
type Journal struct {
	r chan *iop // reads are issued here, pointer b.c. user modifies it.
	w chan iop  // writes are issued here.
	q chan bool // quit signal.
}

// iop represents an I/O request.  err is used to signal the result back
// to the client.
type iop struct {
	mut string
	err chan error
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
	
	j = &Journal{r: make(chan *iop), w: make(chan iop), q: make(chan bool)}
	go iops(j, r, w)
	return
}

// Store writes the mutation to the Journal.
func (j Journal) Store(mutation string) (err error) {
	req := iop{mutation, make(chan error)}
	j.w <- req
	return <-req.err
}

// Retrieve reads the next mutation from the Journal.  It returns
// the mutation and an error, if any.  EOF is signaled by a nil
// mutation with err set to io.EOF
func (j Journal) Retrieve() (mut string, err error) {
	req := iop{err: make(chan error)}
	j.r <- &req
	return req.mut, <-req.err
}

// Close shuts down the journal.
func (j Journal) Close() {
	j.q <- true
}

// iops sits in a loop and processes requests sent by Store and Retrieve.
// Clients of this function specify a channel where it can send back
// the result of the operation. 
func iops(j *Journal, r io.ReadCloser, w io.WriteCloser) {
	defer r.Close()
	defer w.Close()
	for {
		select {
		case rop := <-j.r:
			mut, err := ReadMutation(r)
			rop.mut = mut
			rop.err <- err

		case wop := <-j.w:
			wop.err <- WriteMutation(w, wop.mut)

		case <-j.q:
			return
		}
	}
	return
}

// ReadMutation reads a block from the reader, decodes it into a
// mutation and returns it along with an error.
func ReadMutation(r io.Reader) (mut string, err error) {
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

// WriteMutation encodes a mutation into a block and writes the
// block to the writer returning an error.
func WriteMutation(w io.Writer, mut string) (err error) {
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
