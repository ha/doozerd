
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

const (
	Nop = iota
	Invite
	Rsvp
	Nominate
	Vote
)

type Message interface {
    From() int
    Cmd() int
    Seqn() uint64
    Body() string // soon to be []byte

	SetFrom(byte)
	SetSeqn(uint64)
}

func NewMessage(b []byte) Message {
	s := string(b)
	parts := strings.Split(s, ":", mNumParts)
	if len(parts) != mNumParts {
		panic(s)
	}

	seqn, err := strconv.Btoui64(parts[mSeqn], 10)
	if err != nil {
		panic(s)
	}

	from, err := strconv.Atoi(parts[mFrom])
	if err != nil {
		panic(s)
	}

	cmd, err := strconv.Atoi(parts[mCmd])
	if err != nil {
		panic(s)
	}

	return &Msg{byte(from), byte(cmd), seqn, parts[mBody]}
}

func NewInvite(crnd uint64) Message {
	return &Msg{
		cmd:  Invite,
		body: fmt.Sprintf("%d", crnd),
	}
}

// Returns the info for `m`. If `m` is not an invite, the result is undefined.
func InviteParts(m Message) (crnd uint64) {
	crnd, _ = strconv.Atoui64(m.Body())
	return
}

func NewNominate(crnd uint64, v string) Message {
	return &Msg{
		cmd:  Nominate,
		body: fmt.Sprintf("%d:%s", crnd, v),
	}
}

// Returns the info for `m`. If `m` is not a nominate, the result is undefined.
func NominateParts(m Message) (crnd uint64, v string) {
	parts := strings.Split(m.Body(), ":", 2)
	crnd, _ = strconv.Atoui64(parts[0])
	v = parts[1]
	return
}

func NewRsvp(i, vrnd uint64, vval string) Message {
	return &Msg{
		cmd: Rsvp,
		body: fmt.Sprintf("%d:%d:%s", i, vrnd, vval),
	}
}

// Returns the info for `m`. If `m` is not an rsvp, the result is undefined.
func RsvpParts(m Message) (i, vrnd uint64, vval string) {
	parts := strings.Split(m.Body(), ":", 3)
	i, _ = strconv.Atoui64(parts[0])
	vrnd, _ = strconv.Atoui64(parts[1])
	vval = parts[2]
	return
}

func NewVote(i uint64, vval string) Message {
	return &Msg{
		cmd: Vote,
		body: fmt.Sprintf("%d:%s", i, vval),
	}
}

// Returns the info for `m`. If `m` is not a vote, the result is undefined.
func VoteParts(m Message) (i uint64, vval string) {
	parts := strings.Split(m.Body(), ":", 2)
	i, _ = strconv.Atoui64(parts[0])
	vval = parts[1]
	return
}

// In-memory format:
//
//     0    -- index of sender
//     1    -- cmd
//     2..9 -- seqn
//     10.. -- body -- format depends on command
//
// Wire format is same as in-memory format, but without the first byte (the
// sender index). Here it is for clarity:
//
//     0    -- cmd
//     1..8 -- seqn
//     9..  -- body -- format depends on command
//type Msg []byte

type Msg struct {
	from byte
	cmd byte
	seqn uint64
	body string
}

func (m Msg) Seqn() uint64 {
	return m.seqn
}

func (m Msg) From() int {
	return int(m.from)
}

func (m Msg) Cmd() int {
	return int(m.cmd)
}

func (m Msg) Body() string {
	return m.body
}

func (m *Msg) SetFrom(from byte) {
	m.from = from
}

func (m *Msg) SetSeqn(seqn uint64) {
	m.seqn = seqn
}
