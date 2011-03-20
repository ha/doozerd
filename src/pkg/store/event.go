package store

import (
	"os"
)

type Event struct {
	Seqn int64
	Path string
	Body string

	// the cas token for `Path` as of this event.
	// undefined if the event does not represent a path operation.
	Cas int64

	// the mutation that caused this event
	Mut string

	Err os.Error

	// retrieves values as defined at `Seqn`
	Getter
}

func (e Event) Desc() string {
	switch {
	case e.IsSet():
		return "set"
	case e.IsDel():
		return "del"
	case e.IsNop():
		return "nop"
	}
	panic("unreachable")
}

// Returns true iff the operation represented by `e` set a path.
//
// Mutually exclusive with `IsDel` and `IsNop`.
func (e Event) IsSet() bool {
	return e.Cas > Missing
}

// Returns true iff the operation represented by `e` deleted a path.
//
// Mutually exclusive with `IsSet` and `IsNop`.
func (e Event) IsDel() bool {
	return e.Cas == Missing
}

// Returns true iff `e` does not represent a path operation.
//
// Mutually exclusive with `IsSet` and `IsDel`.
func (e Event) IsNop() bool {
	return e.Cas < Missing
}
