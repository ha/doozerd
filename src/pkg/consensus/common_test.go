package consensus


type msgSlot struct {
	*M
}


func (ms msgSlot) Put(m *M) {
	*ms.M = *m
}
