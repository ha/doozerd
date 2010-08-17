package paxos

type Result struct {
	seqn uint64
	v string
}

type instReq struct {
	seqn uint64
	ch chan *Instance
}

type Manager struct{
	learned chan Result
	reqs chan instReq
	seqns chan uint64
	nodes uint64
}

func NewManager(n uint64) *Manager {
	m := &Manager{
		learned: make(chan Result),
		reqs: make(chan instReq),
		seqns: make(chan uint64),
		nodes: n,
	}
	return m
}

func (m *Manager) Init(outs Putter) {
	go func() {
		instances := make(map[uint64]*Instance)
		for req := range m.reqs {
			inst, ok := instances[req.seqn]
			if !ok {
				quorum := m.nodes/2 + 1
				inst = NewInstance(1, quorum)
				inst.Init(PutWrapper{req.seqn, 1, outs})
				instances[req.seqn] = inst
				go func() {
					m.learned <- Result{req.seqn, inst.Value()}
				}()
			}
			req.ch <- inst
		}
	}()

	// Generate an infinite stream of sequence numbers (seqns).
	go func() {
		for n := uint64(1); ; n++ {
			m.seqns <- n
		}
	}()
}

func (m *Manager) getInstance(seqn uint64) *Instance {
	ch := make(chan *Instance)
	m.reqs <- instReq{seqn, ch}
	return <-ch
}

func (m *Manager) Put(msg Msg) {
	m.getInstance(msg.Seqn).Put(msg)
}

func (m *Manager) Propose(v string) string {
	inst := m.getInstance(<-m.seqns)
	inst.Propose(v)
	return inst.Value()
}

func (m *Manager) Recv() (uint64, string) {
	result := <-m.learned
	return result.seqn, result.v
}

