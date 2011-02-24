package server

type txn struct {
	done   chan bool
	cancel chan bool
}
