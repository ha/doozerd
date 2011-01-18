package paxos

type SyncPutter chan *M

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

func (sp SyncPutter) Put(m *M) {
	sp <- m
}

type msgSlot struct {
	*M
}

func (ms msgSlot) Put(m *M) {
	*ms.M = *m
}

type funcPutter func(m *M)

func (fp funcPutter) Put(m *M) {
	fp(m)
}

type chanPutCloserTo chan Packet

func (cp chanPutCloserTo) PutTo(m *M, addr string) {
	go func() { cp <- Packet{m, addr} }()
}

func (cp chanPutCloserTo) Close() {
	close(cp)
}

