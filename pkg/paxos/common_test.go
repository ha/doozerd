package paxos

import (
	"log"
	"strings"
	"strconv"
	"testing/iotest"
)

type SyncPutter chan Message

var logger = log.New(iotest.TruncateWriter(nil, 0), nil, "", log.Lok)

var tenNodes = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

func (sp SyncPutter) Put(m Message) {
	sp <- m
}

func m(s string) Message {
	parts := strings.Split(s, ":", mNumParts)
	if len(parts) != mNumParts {
		panic(s)
	}

	from, err := strconv.Btoui64(parts[mFrom], 10)
	if err != nil {
		panic(s)
	}

	return Msg{1, from, parts[mCmd], parts[mBody]}
}

func msgs(ss ... string) (messages []Message) {
	messages = make([]Message, len(ss))
	for i, s := range ss {
		messages[i] = m(s)
	}
	return
}
