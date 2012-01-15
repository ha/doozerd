package quiet

import (
	"log"
	"os"
)

func init() {
	dn, err := os.OpenFile(os.DevNull, 0, 0)
	if err != nil {
		panic(err)
	}
	log.SetOutput(dn)
}
