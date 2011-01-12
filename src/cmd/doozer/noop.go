package main

import (
	"doozer/client"
)


func init() {
	cmds["noop"] = cmd{noop, "consensus"}
	cmdHelp["noop"] = `noop [options]

Performs a consensus operation.

No change will be made to the data store.
`
}


func noop() {
	c := client.New("<test>", *addr)

	err := c.Noop()
	if err != nil {
		bail(err)
	}
}
