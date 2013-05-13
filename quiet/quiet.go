package quiet

import (
	"log"
	"io/ioutil"
)

func init() {
	log.SetOutput(ioutil.Discard)
}
