package main

import (
	"doozer/client"
)


func init() {
	cmds["nop"] = cmd{nop, "", "consensus"}
	cmdHelp["nop"] = `Performs a consensus operation.

No change will be made to the data store.
`
}


func nop() {
	c := client.New("<test>", *addr)

	err := c.Nop()
	if err != nil {
		bail(err)
	}
}
