package paxos

import (
	"os"
	"strings"
	"strconv"
)

type Msg struct {
	seqn uint64
	from uint64
	to uint64
	cmd string
	body string
}

const (
	mFrom = iota
	mTo
	mCmd
	mBody
	mNumParts
)

var (
	InvalidArgumentsError = os.NewError("Invalid Arguments")
	Continue = os.NewError("continue")
)

func splitBody(body string, n int) ([]string, os.Error){
	bodyParts := strings.Split(body, ":", n)
	if len(bodyParts) != n {
		return nil, InvalidArgumentsError
	}
	return bodyParts, nil
}

func splitExactly(body string, n int) []string {
	parts, err := splitBody(body, n)
	if err != nil {
		panic(Continue)
	}
	return parts
}

func dtoui64(s string) uint64 {
	i, err := strconv.Btoui64(s, 10)
	if err != nil {
		panic(Continue)
	}
	return i
}

func swallowContinue() {
	p := recover()
	switch v := p.(type) {
	default: panic(p)
	case nil: return // didn't panic at all
	case os.Error:
		switch v {
		default: panic(v)
		case Continue: return
		}
	}
}

