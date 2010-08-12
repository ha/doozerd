package paxos

import (
	"container/vector"
	"strings"
	"strconv"
)

type SyncPutter chan Msg

func (sp SyncPutter) Put(m Msg) {
	sp <- m
}

func gather(ch chan Msg) (got []Msg) {
	var stuff vector.Vector = make([]interface{}, 0)

	for x := range ch {
		stuff.Push(x)
	}

	got = make([]Msg, len(stuff))
	for i, v := range stuff {
		got[i] = v.(Msg)
	}

	return
}

func m(s string) Msg {
	parts := strings.Split(s, ":", mNumParts)
	if len(parts) != mNumParts {
		panic(s)
	}

	from, err := strconv.Btoui64(parts[mFrom], 10)
	if err != nil {
		panic(s)
	}

	var to uint64
	if parts[mTo] == "*" {
		to = 0
	} else {
		to, err = strconv.Btoui64(parts[mTo], 10)
		if err != nil {
			panic(err)
		}
	}

	return Msg{1, from, to, parts[mCmd], parts[mBody]}
}

func msgs(ss ... string) (messages []Msg) {
	messages = make([]Msg, len(ss))
	for i, s := range ss {
		messages[i] = m(s)
	}
	return
}
