package paxos

import (
	"os"
	"strings"
)

type msg struct {
	cmd string
	from uint64
	to uint64
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
)

func splitBody(body string, n int) ([]string, os.Error){
	bodyParts := strings.Split(body, ":", n)
	if len(bodyParts) != n {
		return nil, InvalidArgumentsError
	}
	return bodyParts, nil
}
