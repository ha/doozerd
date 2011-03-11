package gc

import (
	"doozer/consensus"
	"doozer/store"
	"strconv"
	"time"
	"log"
)

func Pulse(node string, seqns <-chan int64, p consensus.Proposer, sleep int64) {
	for {
		seqn := strconv.Itoa64(<-seqns)
		if closed(seqns) {
			break
		}

		e := consensus.Set(p, "/doozer/info/"+node+"/applied", []byte(seqn), store.Clobber)
		if e.Err != nil {
			log.Println(e.Err)
		}

		time.Sleep(sleep)
	}
}
