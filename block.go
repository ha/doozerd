package persistence

type block struct {
	hdr  blockHdr // header describing data.
	data []byte   // payload.
}

type blockHdr struct {
	score [20]byte // SHA-1(block.data).
	size  int      // len(block.data).
}
