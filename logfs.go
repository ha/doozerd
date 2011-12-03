// Package logfs provides basic blocks to build streams of transactions.
package logfs

import "os"

// A Record is the fundamental unit you read, write or delete.
type Record struct {
	Data []byte
	Seqn uint64
}

// Logfs is used for issuing I/O to the backing file.
type Logfs struct {
	file os.File
}
