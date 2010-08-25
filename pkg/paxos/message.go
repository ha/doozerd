
package paxos

import (
    "strconv"
    "strings"
)

const (
	mSeqn = iota
	mFrom
	mTo
	mCmd
	mBody
	mNumParts
)

type Message interface {
    Seqn() uint64
    From() uint64
    Cmd() string
    Body() string // soon to be []byte
}

func parse(s string) Msg {
	parts := strings.Split(s, ":", mNumParts)
	if len(parts) != mNumParts {
		panic(s)
	}

	seqn, err := strconv.Btoui64(parts[mSeqn], 10)
	if err != nil {
		panic(s)
	}

	from, err := strconv.Btoui64(parts[mFrom], 10)
	if err != nil {
		panic(s)
	}

	return Msg{seqn, from, parts[mCmd], parts[mBody]}
}
