
package paxos

import (
    "fmt"
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

func NewMessage(s string) Message {
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

func NewInvite(crnd int) Message {
	return Msg{
		cmd:  "INVITE",
		body: fmt.Sprintf("%d", crnd),
	}
}

func NewNominate(crnd int, v string) Message {
	return Msg{
		cmd:  "NOMINATE",
		body: fmt.Sprintf("%d:%s", crnd, v),
	}
}

// TODO fix these numeric types
func NewRsvp(i, vrnd uint64, vval string) Message {
	return Msg{
		cmd: "RSVP",
		body: fmt.Sprintf("%d:%d:%s", i, vrnd, vval),
	}
}

type Msg struct {
	seqn uint64
	from uint64
	cmd string
	body string
}

func (m Msg) Seqn() uint64 {
	return m.seqn
}

func (m Msg) From() uint64 {
	return m.from
}

func (m Msg) Cmd() string {
	return m.cmd
}

func (m Msg) Body() string {
	return m.body
}
