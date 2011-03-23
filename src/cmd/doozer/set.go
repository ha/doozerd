package main

import (
	"doozer/client"
	"fmt"
	"io/ioutil"
	"os"
)


func init() {
	cmds["set"] = cmd{set, "<path> <rev>", "write a file"}
	cmdHelp["set"] = `Sets the body of the file at <path>.

The body is read from stdin. If <rev> is not greater than or equal to
the revision of the file, no change will be made.

Prints the new revision on stdout, or an error message on stderr.
`
}


func set(path, rev string) {
	oldRev := mustAtoi64(rev)

	c := client.New("<test>", *addr)

	body, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		bail(err)
	}

	newRev, err := c.Set(path, oldRev, body)
	if err != nil {
		bail(err)
	}

	fmt.Println(newRev)
}
