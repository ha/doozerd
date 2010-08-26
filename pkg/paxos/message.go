
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

	SetFrom(byte)
	SetSeqn(uint64)
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

	return &Msg{seqn, from, parts[mCmd], parts[mBody]}
}

func NewInvite(crnd int) Message {
	return &Msg{
		cmd:  "INVITE",
		body: fmt.Sprintf("%d", crnd),
	}
}

// Returns the info for `m`. If `m` is not an invite, the result is undefined.
func InviteParts(m Message) (crnd int) {
	crnd, _ = strconv.Atoi(m.Body())
	return
}

func NewNominate(crnd int, v string) Message {
	return &Msg{
		cmd:  "NOMINATE",
		body: fmt.Sprintf("%d:%s", crnd, v),
	}
}

// Returns the info for `m`. If `m` is not a nominate, the result is undefined.
func NominateParts(m Message) (crnd int, v string) {
	parts := strings.Split(m.Body(), ":", 2)
	crnd, _ = strconv.Atoi(parts[0])
	v = parts[1]
	return
}

// TODO fix these numeric types
func NewRsvp(i, vrnd uint64, vval string) Message {
	return &Msg{
		cmd: "RSVP",
		body: fmt.Sprintf("%d:%d:%s", i, vrnd, vval),
	}
}

// Returns the info for `m`. If `m` is not an rsvp, the result is undefined.
// TODO fix these numeric types
func RsvpParts(m Message) (i, vrnd uint64, vval string) {
	parts := strings.Split(m.Body(), ":", 3)
	i, _ = strconv.Atoui64(parts[0])
	vrnd, _ = strconv.Atoui64(parts[1])
	vval = parts[2]
	return
}

// TODO fix these numeric types
func NewVote(i uint64, vval string) Message {
	return &Msg{
		cmd: "VOTE",
		body: fmt.Sprintf("%d:%s", i, vval),
	}
}

// Returns the info for `m`. If `m` is not a vote, the result is undefined.
// TODO fix these numeric types
func VoteParts(m Message) (i uint64, vval string) {
	parts := strings.Split(m.Body(), ":", 2)
	i, _ = strconv.Atoui64(parts[0])
	vval = parts[1]
	return
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

func (m *Msg) SetFrom(from byte) {
	m.from = uint64(from)
}

func (m *Msg) SetSeqn(seqn uint64) {
	m.seqn = seqn
}
