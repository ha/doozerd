package gc

import (
	"doozer/consensus"
	"doozer/store"
	"strconv"
	"time"
	"log"
)

func Pulse(node string, seqns <-chan int64, p consensus.Proposer, sleep int64) {
	path := "/doozer/info/" + node + "/applied"
	for {
		seqn := strconv.Itoa64(<-seqns)
		if closed(seqns) {
			break
		}

		e := consensus.Set(p, path, []byte(seqn), store.Clobber)
		if e.Err != nil {
			log.Println(e.Err)
		}

		time.Sleep(sleep)
	}
}
