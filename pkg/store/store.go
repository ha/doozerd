package store

type Event struct {
	Type int
	Path string
	Value string
}

const (
	Set = (1<<iota)
	Del
	Add
	Rem
)

func Encode(path, value string) (mutation string)

func decode(mutation string) (path, value string)

func Apply(seqn uint64, mutation string)

// For a missing path, `ok == false`. Otherwise, it is `true`.
func Lookup(path string) (value string, ok bool)

// `eventMask` is one or more of `Set`, `Del`, `Add`, and `Rem`, bitwise OR-ed
// together.
func Watch(path string, eventMask byte) (events chan Event)
