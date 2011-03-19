package main

import (
	"doozer/client"
	"fmt"
)


func init() {
	cmds["rev"] = cmd{rev, "<path>", "read a file"}
	cmdHelp["rev"] = "Prints the current revision.\n"
}


func rev() {
	c := client.New("<test>", *addr)

	rev, err := c.Rev()
	if err != nil {
		bail(err)
	}

	fmt.Println(rev)
}
