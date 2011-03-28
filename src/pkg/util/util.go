package util

import (
	"os"
)

type NullWriter struct{}

func (nw NullWriter) Write(p []byte) (int, os.Error) {
	return len(p), nil
}
