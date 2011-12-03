package logfs

// On disk format for logfs.

// A superBlock describes where the stream of data begins. There is
// a superblock at the start of the backing file and a backup
// superblock at offset 4K.
type superBlock struct {
	sum    uint64 // Check sum for offset
	offset uint64 // File offset for the first record
}

// A record is the fundamental unit for Read/Write/Delete.
type record struct {
	header recordHeader // Records information about data.
	data   []byte       // Passed by Write, returned to Read.
	next   uint64       // File offset of the next record
}

// recordHeader describes a record.  record is variable sized, so this
// structure is fixed size to be read with a single read call before
// reading the actual data.
type recordHeader struct {
	headerSum uint64 // Own checksum.
	dataSum   uint64 // record.data checksum.
	dataLen   uint64 // record.data length.
	seqn      uint64 // Sequence number, must grow in the list.
}
