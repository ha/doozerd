package store

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
