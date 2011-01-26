package gc

import (
	"doozer/paxos"
	"doozer/store"
	"doozer/util"
	"os"
	"strconv"
	"time"
)

func Pulse(node string, seqns <-chan int64, p paxos.Proposer, sleep int64) {
	logger := util.NewLogger("pulse")

	var err os.Error
	cas := store.Missing

	for {
		seqn := strconv.Itoa64(<-seqns)
		if closed(seqns) {
			break
		}

		_, cas, err = paxos.Set(p, "/doozer/info/"+node+"/applied", seqn, cas, nil)
		if err != nil {
			logger.Println(err)
		}

		time.Sleep(sleep)
	}
}
