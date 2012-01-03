// Package persistence allows doozer to save state across server restarts.
// Doozer mutations are apended to the file on disk.  Deletions are
// garbage collected from the head of the log file so it does not
// grow indefinitely.

package persistence

import (
	"encoding/binary"
	"errors"
	"io"
)

// Journal represents a file where doozer can save state.
type Journal struct {
	r chan *iop // reads are issued here, pointer b.c. user modifies it.
	w chan iop  // writes are issued here.
	q chan bool // quit signal.
}

// iop represents an I/O request.  err is used to signal the result back
// to the client.  mut is filled by the client, in case of writes and
// by the system in case of reads.
type iop struct {
	mut string
	err chan error
}

// NewJournal opens the named file for synchronous I/O, creating it
// with mode 0640 if it does not exist and prepares it for logging operation.
// If successful, methods on the returned Journal can be used for I/O.
// It returns a Journal and an error, if any.
func NewJournal(name string) (j Journal, err error) {
	j = Journal{r: make(chan *iop), w: make(chan iop), q: make(chan bool)}
	arena, err := newArena(name)
	if err != nil {
		return
	}
	go iops(arena, j)
	return
}

// Store writes the mutation to the Journal.
func (j Journal) Store(mutation string) (err error) {
	req := iop{mutation, make(chan error)}
	j.w <- req
	err = <-req.err
	return
}

// Retrieve reads the next mutation from the Journal.  It returns
// the mutation and an error, if any.  EOF is signaled by a nil
// mutation with err set to io.EOF
func (j Journal) Retrieve() (mut string, err error) {
	req := iop{err: make(chan error)}
	j.r <- &req
	err = <-req.err
	mut = req.mut
	return
}

// Close shuts down the journal.
func (j Journal) Close() {
	j.q <- true
}

// iops sits in a loop and processes requests sent by Store and Retrieve.
// Clients of this function specify a channel where it can send back
// the result of the operation. 
func iops(rw io.ReadWriteCloser, j Journal) {
	defer rw.Close()
	for {
		select {
		case rop := <-j.r:
			mut, err := decodedRead(rw)
			rop.mut = mut
			rop.err <- err

		case wop := <-j.w:
			wop.err <- encodedWrite(rw, wop.mut)

		case <-j.q:
			return
		}
	}
	return
}

// decodedRead reads a block from the reader, decodes it into a
// mutation and returns it along with an error.
func decodedRead(r io.Reader) (mut string, err error) {
	b := block{}

	// Read the header so we know how much to read next.
	err = binary.Read(r, binary.LittleEndian, &b.Hdr)
	if err != nil {
		return
	}

	// If everything went fine, we can read the data.
	_, err = io.ReadAtLeast(r, b.Data, int(b.Hdr.Size))

	// We need to make sure the checksum is valid.
	if b.isValid() != true {
		err = errors.New("checksum check failed")
		return
	}

	mut = string(b.Data)
	return
}

// encodedWrite encodes a mutation into a block and writes the
// block to the writer returning an error.
func encodedWrite(w io.Writer, mut string) (err error) {
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
