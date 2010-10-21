package junta

import (
	"os"
)

type Proposer interface {
	Propose(v string) (uint64, string, os.Error)
}
