package paxos

import (
	"container/vector"
	"strings"
	"strconv"
)

func gather(ch chan msg) (got []msg) {
	var stuff vector.Vector = make([]interface{}, 0)

	for x := range ch {
		stuff.Push(x)
	}

	got = make([]msg, len(stuff))
	for i, v := range stuff {
		got[i] = v.(msg)
	}

	return
}

func m(s string) msg {
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

	return msg{parts[mCmd], from, to, parts[mBody]}
}

func msgs(ss ... string) (messages []msg) {
	messages = make([]msg, len(ss))
	for i, s := range ss {
		messages[i] = m(s)
	}
	return
}
