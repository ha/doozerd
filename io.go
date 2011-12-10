package logfs

import "os"

// iop (I/O packet) encodes a physical I/O operation.
type iop struct {
	b   *block          // data, in on-disk format.
	err chan<- os.Error // channel to return result to caller.
}

// writer receives iops from c, tries to write data to disk and
// sends the result of the write back to the issuer.
func (l *Logfs) writer(c <-chan iop, quit <-chan bool) {
	for {
		select {
		case op := <-c:
			_ = op
			panic("not implemented")
		case <-quit:
			return
		}
	}
}
