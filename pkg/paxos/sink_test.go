package paxos

import (
	"junta/assert"
	"testing"
)

func sink(ch chan Msg) string {
	for m := range ch {
		switch m.Cmd() {
		case learn:
			return learnParts(m)
		}
	}
	return ""
}

func TestSinkLearnsAValue(t *testing.T) {
	msgs := make(chan Msg)
	taught := make(chan string)

	go func() {
		taught <- sink(msgs)
	}()

	msgs <- newLearn("foo")

	assert.Equal(t, "foo", <-taught, "")
}

func TestSinkIgnoresOtherMessages(t *testing.T) {
	msgs := make(chan Msg)
	taught := make(chan string)

	go func() {
		taught <- sink(msgs)
	}()

	msgs <- newVote(1, "foo")
	msgs <- newLearn("foo")

	assert.Equal(t, "foo", <-taught, "")
}

func TestSinkExitsQuietly(t *testing.T) {
	msgs := make(chan Msg)
	taught := make(chan string)

	go func() {
		taught <- sink(msgs)
	}()

	close(msgs)

	assert.Equal(t, "", <-taught, "")
}
