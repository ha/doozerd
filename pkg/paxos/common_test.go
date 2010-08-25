package paxos

import (
	"log"
	"testing/iotest"
)

type SyncPutter chan Message

var logger = log.New(iotest.TruncateWriter(nil, 0), nil, "", log.Lok)

var tenNodes = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

func (sp SyncPutter) Put(m Message) {
	sp <- m
}

func m(s string) Message {
	return parse("1:" + s)
}

func msgs(ss ... string) (messages []Message) {
	messages = make([]Message, len(ss))
	for i, s := range ss {
		messages[i] = m(s)
	}
	return
}
