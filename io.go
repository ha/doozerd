package logfs

import "os"

// iop (I/O packet) encodes a physical I/O operation.
type iop struct {
	b   *block          // data, in on-disk format.
	err chan<- os.Error // channel to return result to caller.
}

// writer receives iops from l.w, tries to write data to disk and
// sends the result of the write back to the issuer.
func (l *Logfs) writer() {
	for {
		select {
		case op := <-l.w:
			_ = op
			panic("not implemented")
		case <-l.quitw:
			return
		}
	}
}
