package main

import (
	"doozer/client"
	"fmt"
	"os"
)


func init() {
	cmds["watch"] = cmd{watch, "<glob>", "get notified of changes"}
	cmdHelp["watch"] = `Prints changes to each file matching <glob>.

Rules for <glob> pattern-matching:
 - '?' matches a single char in a single path component
 - '*' matches zero or more chars in a single path component
 - '**' matches zero or more chars in zero or more components
 - any other sequence matches itself

Output is a sequence of records, one for each change. Format of each record:

  <path> <rev> <len> LF <body> LF

Here, <path> is the file's path, <rev> is the revision, <len> is the number of
bytes in the body, <body> is the bytes of the body, and LF is an ASCII
line-feed char.

If a file is deleted, <rev> will be 0.
`
}


func watch(glob string) {
	c := client.New("<test>", *addr)

	w, err := c.Watch(glob, 0)
	if err != nil {
		bail(err)
	}

	for ev := range w.C {
		if ev.Err != nil {
			fmt.Fprintln(os.Stderr, ev.Err)
		}

		fmt.Println(ev.Path, ev.Rev, len(ev.Body))
		os.Stdout.Write(ev.Body)
		fmt.Println()
	}
}
