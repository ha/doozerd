package consensus

import (
	"doozer/store"
)


type Run struct{}

func GenerateRuns(st *store.Store, runs chan<- Run) {
	runs <- Run{}
}
