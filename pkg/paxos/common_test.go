package paxos

type SyncPutter chan Msg

var tenNodes = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}

func (sp SyncPutter) Put(m Msg) {
	sp <- m
}
