package persistence

import (
	"bytes"
	"crypto/sha1"
	"io"
)

// blocks are written to disk.
type block struct {
	hdr  blockHdr // header describing data.
	data []byte   // payload.
}

// blockHdr describes a block.
type blockHdr struct {
	score [20]byte // SHA-1(block.data).
	size  int      // len(block.data).
}

// newBlock returns a block created from a mutation.
func newBlock(mutation string) (b block) {
	b.hdr.size = len(b.data)

	sha1 := sha1.New()
	io.WriteString(sha1, mutation)
	copy(b.hdr.score[:], sha1.Sum(nil))

	b.data = []byte(mutation)

	return
}

// isValid validates the checksum of a block.
func (b block) isValid() bool {
	sha1 := sha1.New()
	sha1.Write(b.data[:b.hdr.size])
	return bytes.Equal(b.hdr.score[:], sha1.Sum(nil))
}
