package paxos

type SyncPutter chan Msg

var tenNodes = map[string]string{
	"a":"p",
	"b":"q",
	"c":"r",
	"d":"s",
	"e":"t",
	"f":"u",
	"g":"v",
	"h":"w",
	"i":"x",
	"j":"y",
}

func (sp SyncPutter) Put(m Msg) {
	sp <- m
}

func newClusterWithId(self string, addrsById map[string]string) *cluster {
	cx := newCluster(addrsById)
	cx.self = self
	return cx
}
