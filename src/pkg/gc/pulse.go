package gc

import (
	"doozer/consensus"
	"doozer/store"
	"strconv"
	"time"
	"log"
)

func Pulse(node string, seqns <-chan int64, p consensus.Proposer, sleep int64) {
	path := "/ctl/node/" + node + "/applied"
	for {
		seqn, ok := <-seqns
		if !ok {
			break
		}

		e := consensus.Set(p, path, []byte(strconv.Itoa64(seqn)), store.Clobber)
		if e.Err != nil {
			log.Println(e.Err)
		}

		time.Sleep(sleep)
	}
}
