package consensus


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

