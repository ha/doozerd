package logfs

// On disk format for logfs.

// A block is the fundamental unit on disk.  Each Record translates
// to a block on disk.
type block struct {
	header blockHeader // Records information about data.
	data   []byte       // Passed by Write, returned to Read.
	next   uint64       // File offset of the next block
}

// blockHeader describes a block.  A block is variable sized, so this
// structure is fixed size to be read with a single read call before
// reading the actual data.
type blockHeader struct {
	headerSum  uint64 // Own checksum.
	cookie     uint64 // To be matched with previous block.
	nextCookie uint64 // Next block's cookie must match this.
	dataSum    uint64 // block.data checksum.
	dataLen    uint64 // block.data length.
	seqn       uint64 // Sequence number, must grow in the list.
}
