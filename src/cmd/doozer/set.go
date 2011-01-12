package main

import (
	"doozer/client"
	"fmt"
	"io/ioutil"
	"os"
)


func init() {
	cmds["set"] = cmd{set, "<path> <cas>", "write a file"}
	cmdHelp["set"] = `Sets the body of the file at <path>.

The body is read from stdin. If <cas> does not match the existing CAS token of
the file, no change will be made.

Prints the new CAS token on stdout, or an error message on stderr.
`
}


func set(path, cas string) {
	oldCas := mustAtoi64(cas)

	c := client.New("<test>", *addr)

	body, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		bail(err)
	}

	newCas, err := c.Set(path, oldCas, body)
	if err != nil {
		bail(err)
	}

	fmt.Println(newCas)
}
