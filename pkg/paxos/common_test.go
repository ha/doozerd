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

var tenIds = stringKeys(tenNodes)

func (sp SyncPutter) Put(m Msg) {
	sp <- m
}
