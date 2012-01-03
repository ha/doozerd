package persistence

import (
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

// newBlock returns a block created from mutation.
func newBlock(mutation string) (b *block) {
	b.hdr.size = len(b.data)

	sha1 := sha1.New()
	io.WriteString(sha1, mutation)
	scoreCpy(b.hdr.score[:], sha1.Sum(nil))

	b.data = []byte(mutation)

	return
}

// scoreCpy copies sum into score.
func scoreCpy(score, sum []byte) {
	if len(score) != len(sum) {
		panic("checksum length mismatch")
	}

	for i, _ := range score {
		score[i] = sum[i]
	}
}

// scoreCmp does a C-style compare between sum and score.
func scoreCmp(score, sum []byte) int {
	switch {
	case len(score) > len(sum):
		return 1
	case len(score) < len(sum):
		return -1

	}

	for i, _ := range score {
		switch {
		case score[i] > sum[i]:
			return 1
		case score[i] < sum[i]:
			return -1
		}
	}

	return 0
}

func (b block) String() string {
	return string(b.data)
}

// isValid validates the checksum of a block.
func (b block) isValid() bool {
	sha1 := sha1.New()
	sha1.Write(b.data[:b.hdr.size])
	if scoreCmp(b.hdr.score[:], sha1.Sum(nil)) != 0 {
		return false
	}

	return true
}
