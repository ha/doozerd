package gc

import (
	"doozer/store"
	"doozer/util"
	"os"
	"strconv"
	"time"
)

type Setter interface {
	Set(path, body, oldCas string) (newCas string, err os.Error)
}

type Seqencer interface {
	Seqn() uint64
}

func Pulse(node string, sr Seqencer, s Setter, sleep int64) {
	logger := util.NewLogger("pulse")

	var err os.Error
	cas := store.Missing

	for {
		seqn := strconv.Uitoa64(sr.Seqn())
		cas, err = s.Set("/doozer/info/" + node + "/applied", seqn, cas)
		if err != nil {
			logger.Println(err)
		}
		time.Sleep(sleep)
	}

	panic("unreachable")
}

