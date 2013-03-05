package quiet

import (
	"log"
	"os"
)

func init() {
	// TODO: if this is switched to ioutil.Discard a few of the tests hang.
	// Presumably this means that there are legitimate deadlock issues 
	// somewhere in the core...
	// OSX 10.8.2 go1.0.3
	// (GOMAXPROCS > 1 also resolves the issue indicating the same)
	dn, err := os.OpenFile(os.DevNull, 0, 0)
	if err != nil {
		panic(err)
	}
	log.SetOutput(dn)
}
