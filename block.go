package persistence

import (
	"bytes"
	"crypto/sha1"
	"io"
)

// blocks are written to disk.
type block struct {
	Hdr  blockHdr // header describing data.
	Data []byte   // payload.
}

// blockHdr describes a block.
type blockHdr struct {
	Score [20]byte // SHA-1(block.data).
	Size  int32    // len(block.data).
}

// newBlock returns a block created from a mutation.
func newBlock(mutation string) (b block) {
	sha1 := sha1.New()
	io.WriteString(sha1, mutation)
	copy(b.Hdr.Score[:], sha1.Sum(nil))

	b.Data = []byte(mutation)
	b.Hdr.Size = int32(len(b.Data))

	return
}

// isValid validates the checksum of a block.
func (b block) isValid() bool {
	sha1 := sha1.New()
	sha1.Write(b.Data[:b.Hdr.Size])
	return bytes.Equal(b.Hdr.Score[:], sha1.Sum(nil))
}
