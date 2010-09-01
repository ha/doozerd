package paxos

const window = 50

type result struct {
	seqn uint64
	v    string
}

type instReq struct {
	seqn uint64 // 0 means to generate a fresh seqn
	ch   chan *instance
}

type Manager struct {
	rg      *Registrar
	learned chan result
	reqs    chan instReq
}

func (m *Manager) process(next uint64, outs Putter) {
	instances := make(map[uint64]*instance)
	for req := range m.reqs {
		if req.seqn == 0 {
			req.seqn = next
		}
		inst, ok := instances[req.seqn]
		if !ok {
			// TODO find a nicer way to do this
			// This is meant to be run in a separate goroutine
			cxf := func() *cluster {
				return m.rg.clusterFor(req.seqn)
			}
			inst = newInstance(cxf, putWrapper{req.seqn, outs})
			instances[req.seqn] = inst
			go func() {
				m.learned <- result{req.seqn, inst.Value()}
			}()
		}
		req.ch <- inst
		if req.seqn >= next {
			next = req.seqn + 1
		}
	}
}

func NewManager(start uint64, rg *Registrar, outs Putter) *Manager {
	m := &Manager{
		rg:      rg,
		learned: make(chan result),
		reqs:    make(chan instReq),
	}

	go m.process(start, outs)

	return m
}

func (m *Manager) getInstance(seqn uint64) *instance {
	ch := make(chan *instance)
	m.reqs <- instReq{seqn, ch}
	return <-ch
}

func (m *Manager) Put(msg Msg) {
	if !msg.Ok() {
		return
	}
	m.getInstance(msg.Seqn()).Put(msg)
}

func (m *Manager) PutFrom(addr string, msg Msg) {
	msg.SetFrom(m.getInstance(msg.Seqn()).cluster().indexByAddr(addr))
	m.Put(msg)
}

func (m *Manager) AddrsFor(msg Msg) []string {
	return m.getInstance(msg.Seqn()).cluster().addrs()
}

func (m *Manager) Propose(v string) string {
	inst := m.getInstance(0)
	logger.Logf("paxos propose -> %q", v)
	inst.Propose(v)
	return inst.Value()
}

func (m *Manager) Recv() (uint64, string) {
	result := <-m.learned
	logger.Logf("paxos %d learned <- %q", result.seqn, result.v)
	return result.seqn, result.v
}
