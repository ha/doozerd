package main

import (
	"doozer/client"
	"fmt"
	"os"
)


func init() {
	cmds["find"] = cmd{find, "<glob>", "list files"}
	cmdHelp["find"] = `Prints the tree matching <glob>

Rules for <glob> pattern-matching:
 - '?' matches a single char in a single path component
 - '*' matches zero or more chars in a single path component
 - '**' matches zero or more chars in zero or more components
 - any other sequence matches itself

Prints a sequence of paths, one for each file/directory. Format of each record:

  <path> LF

Here, <path> is the file's path, and LF is an ASCII line-feed char.
`
}


func find(glob string) {
	c := client.New("<test>", *addr)

	if glob[len(glob)-1:] != "/" {
		glob = glob + "/"
	}

	w, err := c.Walk(glob+"**", 0, nil, nil)
	if err != nil {
		bail(err)
	}

	for ev := range w.C {
		if ev.Err != nil {
			fmt.Fprintln(os.Stderr, ev.Err)
		}

		fmt.Println(ev.Path)
	}
}
