package store

import (
	"os"
)

type Event struct {
	Seqn  uint64
	Path  string
	Body  string

	// the cas token for `Path` as of this event.
	// undefined if the event does not represent a path operation.
	Cas   string

	// the mutation that caused this event
	Mut   string

	Err   os.Error
}

func (e Event) Desc() string {
	switch {
	case e.IsSet():
		return "set"
	case e.IsDel():
		return "del"
	case e.IsDummy() && e.Mut == Nop:
		return "nop"
	case e.IsDummy() && e.Mut == "":
		return "dummy"
	case e.IsDummy() && e.Mut != "" && e.Mut != Nop:
		return "snapshot"
	}
	panic("unreachable")
}

// Returns true iff the operation represented by `e` set a path.
//
// Mutually exclusive with `IsDel` and `IsDummy`.
func (e Event) IsSet() bool {
    return !e.IsDel() && !e.IsDummy()
}

// Returns true iff the operation represented by `e` deleted a path.
//
// Mutually exclusive with `IsSet` and `IsDummy`.
func (e Event) IsDel() bool {
    return e.Cas == Missing
}

// Returns true iff `e` does not represent a path operation. For example,
// applying a snapshot or waiting on a previously applied seqn will generate
// such an event.
//
// Mutually exclusive with `IsSet` and `IsDel`.
func (e Event) IsDummy() bool {
    return e.Path == ""
}
