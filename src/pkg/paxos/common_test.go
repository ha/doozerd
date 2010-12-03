package paxos

type SyncPutter chan Msg

var tenNodes = map[string]string{
	"a": "p",
	"b": "q",
	"c": "r",
	"d": "s",
	"e": "t",
	"f": "u",
	"g": "v",
	"h": "w",
	"i": "x",
	"j": "y",
}

var tenIds = make([]string, len(tenNodes))

func init() {
	i := 0
	for id := range tenNodes {
		tenIds[i] = id
		i++
	}
}

func (sp SyncPutter) Put(m Msg) {
	sp <- m
}

type msgSlot struct {
	*Msg
}

func (ms msgSlot) Put(m Msg) {
	*ms.Msg = m
}

type funcPutter func(m Msg)

func (fp funcPutter) Put(m Msg) {
	fp(m)
}
