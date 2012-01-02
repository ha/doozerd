package persistence

import (
	"crypto/sha1"
	"io"
)

type block struct {
	hdr  blockHdr // header describing data.
	data []byte   // payload.
}

type blockHdr struct {
	score []byte // SHA-1(block.data), 20 bytes.
	size  int    // len(block.data).
}

func newBlock(mutation string) (b *block) {
	b.data = []byte(mutation)
	b.hdr.size = len(b.data)
	sha1 := sha1.New()
	io.WriteString(sha1, mutation)
	b.hdr.score = sha1.Sum(nil)

	return
}

func (b block)String() string {
	return string(b.data)
}
