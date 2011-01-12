package main

import (
	"doozer/client"
)


func init() {
	cmds["del"] = cmd{del, "<path> <cas>", "delete a file"}
	cmdHelp["del"] = `Deletes the file at <path>.

If <cas> does not match the existing CAS token of the file,
no change will be made.
`
}


func del(path, cas string) {
	oldCas := mustAtoi64(cas)

	c := client.New("<test>", *addr)

	err := c.Del(path, oldCas)
	if err != nil {
		bail(err)
	}
}
