package gc

import (
	"doozer/consensus"
	"doozer/store"
	"doozer/util"
	"strconv"
	"time"
)

func Pulse(node string, seqns <-chan int64, p consensus.Proposer, sleep int64) {
	logger := util.NewLogger("pulse")

	var e store.Event
	for {
		seqn := strconv.Itoa64(<-seqns)
		if closed(seqns) {
			break
		}

		e = consensus.Set(p, "/doozer/info/"+node+"/applied", []byte(seqn), e.Cas)
		if e.Err != nil {
			logger.Println(e.Err)
		}

		time.Sleep(sleep)
	}
}
