package server

type txn struct {
	done   chan bool
	cancel chan bool
}


func newTxn() txn {
	return txn{make(chan bool), make(chan bool, 1)}
}
