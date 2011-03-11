package session

import (
	"doozer/consensus"
	"doozer/store"
	"strconv"
)

var sessions = store.MustCompileGlob("/session/*")


// Clean receives nanosecond time values from t. For each time
// received, Clean reads the files in /session in s (interpreting each
// file's body as a decimal integer time value, in nanoseconds), and
// deletes each file with a time less than the time received from t.
//
// If a file does not contain a well-formed decimal integer, its
// time is taken to be 0.
//
// Parameter t can be the output chan of a time.Ticker.
func Clean(s *store.Store, p consensus.Proposer, t <-chan int64) {
	for now := range t {
		delAll(p, expired(s, now))
	}
}


func delAll(p consensus.Proposer, files map[string]int64) {
	for path, cas := range files {
		consensus.Del(p, path, cas)
	}
}


func expired(g store.Getter, now int64) map[string]int64 {
	exps := make(map[string]int64)
	store.Walk(g, sessions, func(path, body string, cas int64) bool {
		// on err, t==0, which is just what we want.
		if t, _ := strconv.Atoi64(body); t < now {
			exps[path] = cas
		}

		return false
	})
	return exps
}
