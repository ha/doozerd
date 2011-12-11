package logfs

// iop (I/O packet) encodes a physical I/O operation.
type iop struct {
	b   *block       // data, in on-disk format.
	err chan<- error // channel to return result to caller.
}

// writer receives iops from l.w, tries to write data to disk and
// sends the result of the write back to the issuer.
func (l *Logfs) writer() {
	for {
		select {
		case op := <-l.w:
			_ = op
			panic("writer: not implemented.")
		case <-l.quitw:
			return
		}
	}
}

// reader receives iops from l.r, tries to read from the disk
// in op.b and sends the result of the read back to the issuer.
func (l *Logfs) reader() {
	for {
		select {
		case op := <-l.r:
			_ = op
			panic("reader: not implemented.")
		case <-l.quitr:
			return
		}
	}
}
