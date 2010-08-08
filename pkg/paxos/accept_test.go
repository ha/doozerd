package paxos

import (
	"borg/assert"
	"testing"
	"container/vector"
	"strings"
	"strconv"
	"fmt"
)

// Helpers
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

// End Helpers

func TestAcceptsInvite(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	go accept(2, ins, outs)
	ins <- m("1:*:INVITE:1")
	close(ins)

	exp := msgs("2:1:ACCEPT:1:0:")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestInvitesAfterNewInvitesAreStaleAndIgnored(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	go accept(2, ins, outs)
	ins <- m("1:*:INVITE:2")
	ins <- m("1:*:INVITE:1")
	close(ins)

	exp := msgs("2:1:ACCEPT:2:0:")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestIgnoresMalformedMessages(t *testing.T) {
	totest := msgs(
		//m("x"),            // too few separators
		//m("x:x"),          // too few separators
		//m("x:x:x"),        // too few separators
		//m("x:x:x:x:x"),    // too many separators
		//m("1:x:INVITE:1"), // invalid to address
		//m("X:*:INVITE:1"), // invalid from address

		"1:*:INVITE:x", // invalid round number
		"1:*:x:1",      // unknown command
		"1:7:INVITE:1", // valid but incorrect to address

		"1:*:NOMINATE:x",     // too few separators in nominate body
		"1:*:NOMINATE:x:foo", // invalid round number
	)

	for _, test := range totest {
		ins := make(chan msg)
		outs := make(chan msg)

		go accept(2, ins, outs)
		ins <- test
		close(ins)

		exp := []msg{}

		// outs was closed; therefore all messages have been processed
		assert.Equal(t, exp, gather(outs), fmt.Sprintf("#v", test))
	}
}

func TestItVotes(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	val := "foo"

	go accept(2, ins, outs)
	// According to paxos, we can omit Phase 1 in round 1
	ins <- m("1:*:NOMINATE:1:"+val)
	close(ins)

	exp := msgs("2:*:VOTE:1:" + val)

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestItVotesWithAnotherValue(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	val := "bar"

	go accept(2, ins, outs)
	// According to paxos, we can omit Phase 1 in round 1
	ins <- m("1:*:NOMINATE:1:"+val)
	close(ins)

	exp := msgs("2:*:VOTE:1:" + val)

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestItVotesWithAnotherRound(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	val := "bar"

	go accept(2, ins, outs)
	// According to paxos, we can omit Phase 1 in the first round
	ins <- m("1:*:NOMINATE:2:"+val)
	close(ins)

	exp := msgs("2:*:VOTE:2:" + val)

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestItVotesWithAnotherSelf(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	val := "bar"

	go accept(3, ins, outs)
	// According to paxos, we can omit Phase 1 in the first round
	ins <- m("1:*:NOMINATE:2:"+val)
	close(ins)

	exp := msgs("3:*:VOTE:2:" + val)

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestItIgnoresOldNominations(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	val := "bar"

	go accept(3, ins, outs)
	// According to paxos, we can omit Phase 1 in the first round
	ins <- m("1:*:INVITE:2")
	<-outs // throw away ACCEPT message
	ins <- m("1:*:NOMINATE:1:"+val)
	close(ins)

	exp := []msg{}

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestInvitesAfterNewNominationsAreStaleAndIgnored(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	go accept(2, ins, outs)
	ins <- m("1:*:NOMINATE:2:v")
	<-outs // throw away VOTE message
	ins <- m("1:*:INVITE:1")
	close(ins)

	exp := []msg{}

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestVotedRoundsAndValuesAreTracked(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	go accept(2, ins, outs)
	ins <- m("1:*:NOMINATE:1:v")
	<-outs // throw away VOTE message
	ins <- m("1:*:INVITE:2")
	close(ins)

	exp := msgs("2:1:ACCEPT:2:1:v")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}

func TestVotesOnlyOncePerRound(t *testing.T) {
	ins := make(chan msg)
	outs := make(chan msg)

	go accept(2, ins, outs)
	ins <- m("1:*:NOMINATE:1:v")
	ins <- m("1:*:NOMINATE:1:v")
	close(ins)

	exp := msgs("2:*:VOTE:1:v")

	// outs was closed; therefore all messages have been processed
	assert.Equal(t, exp, gather(outs), "")
}
