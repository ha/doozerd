package gc

import (
	"doozer/store"
	"doozer/util"
	"os"
	"strconv"
	"time"
)

type Setter interface {
	Set(path string, oldCas int64, body []byte) (newCas int64, err os.Error)
}

func Pulse(node string, seqns <-chan int64, s Setter, sleep int64) {
	logger := util.NewLogger("pulse")

	var err os.Error
	cas := store.Missing

	for {
		seqn := strconv.Itoa64(<-seqns)
		if closed(seqns) {
			break
		}

		cas, err = s.Set("/doozer/info/"+node+"/applied", cas, []byte(seqn))
		if err != nil {
			logger.Println(err)
		}

		time.Sleep(sleep)
	}
}
