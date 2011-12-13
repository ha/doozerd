package logfs

// On disk format for logfs.

import "encoding/binary"

// A block is the fundamental unit on disk.  Each Record translates
// to a block on disk.
type block struct {
	header blockHeader // Records information about data.
	data   []byte      // Passed by Write, returned to Read.
	next   uint64      // File offset of the next block.
}

// blockHeader describes a block.  A block is variable sized, so this
// header is fixed size to know how much to read.
type blockHeader struct {
	headerSum  uint64 // Own checksum.
	cookie     uint64 // To be matched with previous block.
	nextCookie uint64 // Next block's cookie must match this.
	dataLen    uint64 // block.data length.
	dataSum    uint64 // block.data checksum.
	seqn       uint64 // Sequence number, must grow in the list.
}

// blockWrite issues a write request to the writer, waits for
// the result and returns it.  It multiplexes many clients to one writer.
func (l *Logfs) blockWrite(b *block) error {
	c := make(chan error)
	l.w <- iop{b, c}
	return <-c
}

// blockRead issues a read request to the reader, waits for
// the result and returns it.  It multiplexes many clients
// to one reader.
func (l *Logfs) blockRead() (b *block, err error) {
	c := make(chan error)
	l.r <- iop{b, c}
	return b, <-c
}

// physWriteLink writes the offset of the next block to disk.
// It returns nil after the data has been commited to disk, or an
// error otherwise.
func (l *Logfs)physWriteLink(offset uint64) error {
	return binary.Write(l.wf, binary.LittleEndian, offset)
}

// physWriteBlock writes b to the disk.  It assumes physWriteLink
// has written the offset at the end of the last block.  It returns nil
// after the data has been commited to disk, or an error otherwise.
func (l *Logfs)physWriteBlock(b *block) error {
	err := binary.Write(l.wf, binary.LittleEndian, b.header)
	if err != nil {
		return err
	}
	_, err = l.wf.Write(b.data)
	return err
}

// physWrite writes the next block to the disk.  It first writes a link
// in the last block, then writes b to the disk.
// It returns nil after the data has been commited to disk, or an error
// otherwise.
func (l *Logfs)physWrite(b *block) error {
	err := l.physWriteLink(0) // BUG: NOOP as delete is not implemented.
	if err != nil {
		return err
	}
	_, err = l.wf.Seek(0, 1)  // BUG: NOOP as delete is not implemented.
	return l.physWriteBlock(b)
	
}

// physReadLink reads the next block's offset from the disk.
// If successful, it returns the offset, if an error had occurred
// it returns the error.
func (l *Logfs) physReadLink() (offset uint64, err error) {
	err = binary.Read(l.rf, binary.LittleEndian, &offset)
	return
}

// physReadBlock reads a block from the disk.  If successful, it returns
// the block read, if an error had occurred it returns the error.
func (l *Logfs) physReadBlock() (b *block, err error) {
	err = binary.Read(l.rf, binary.LittleEndian, &b.header)
	if err != nil {
		return
	}
	_, err = l.rf.Read(b.data)
	return
}

// physRead reads the next block from the disk.  It first reads the
// next block's offset, then it reads the block.  It returns the block
// if it was successful, or an error otherwise.
func (l *Logfs) physRead() (b *block, err error) {
	_, err = l.physReadLink()
	if err != nil {
		return
	}
	l.rf.Seek(0, 1) // BUG: NOOP as delete is not implemented.
	return l.physReadBlock()
}
