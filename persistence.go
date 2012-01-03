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
func NewJournal(name string) (j *Journal, err error) {
	j.r = make(chan *iop)
	j.w = make(chan iop)
	j.q = make(chan bool)
	arena, err := newArena(name)
	if err != nil {
		return nil, err
	}
	go iops(arena, j)
	return
}

// Store writes the mutation to the Journal.
func (j *Journal) Store(mutation string) error {
	wrq := iop{mutation, make(chan error)}
	j.w <- wrq
	err := <-wrq.err

	return err
}

// Retrieve reads the next mutation from the Journal.  It returns
// the mutation and an error, if any.  EOF is signaled by a nil
// mutation with err set to io.EOF
func (j *Journal) Retrieve() (mutation string, err error) {
	rrq := iop{err: make(chan error)}
	j.r <- &rrq
	err = <-rrq.err

	return rrq.mut, err
}

// iops sits in a loop and processes requests sent by Store and Retrieve.
// Clients of this function specify a channel where it can send back
// the result of the operation. 
func iops(rw io.ReadWriter, j *Journal) {
	for {
		select {
		// read.
		case rop := <-j.r:
			b := block{}

			// Read the header so we know how much to read next.
			err := binary.Read(rw, binary.LittleEndian, &b.hdr)

			// If everything went fine, we can read the data.
			if err == nil {
				_, err = io.ReadAtLeast(rw, b.data, b.hdr.size)
			}

			// We need to make sure the checksum is valid.
			if !b.isValid() {
				err = errors.New("read an invalid block")
			}

			// Everything went well, copy the data to the client supplied
			// structure and signal the client we are done.
			rop.mut = b.String()
			rop.err <- err

		// write.
		case wop := <-j.w:
			b := newBlock(wop.mut)

			// We use two write calls bacause we use encoding/binary
			// to write the fixed length header.

			err := binary.Write(rw, binary.LittleEndian, b.hdr)

			// We'we written the header successfully, write the reast
			// of the data.
			if err == nil {
				_, err = rw.Write(b.data)
			}

			// Everything went well, we signal the user we are done.
			wop.err <- err

		// quit.
		case <-j.q:
			return
		}
	}
	return
}

