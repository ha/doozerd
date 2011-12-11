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

// reader reads iops from the disk and sends them to l.r.
func (l *Logfs) reader() {
	for {
		select {
		case l.r <- iop{}:
			panic("reader: not implemented.")
		case <-l.quitr:
			return
		}
	}
}
