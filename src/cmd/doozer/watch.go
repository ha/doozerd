package main

import (
	"doozer/client"
	"fmt"
	"os"
)


func init() {
	cmds["watch"] = cmd{watch, "get notified of changes"}
	cmdHelp["watch"] = `watch [options] <glob>

Prints the path, CAS token, and body of each change to each file matching
<glob>.

Prints a sequence of records, one for each change. Format of each record:

  <path> <cas> <len> LF <body> LF

Here, <path> is the file's path, <cas> is the cas token, <len> is the number of
bytes in the body, <body> is the bytes of the body, and LF is an ASCII
line-feed char.

If a file is deleted, <cas> will be 0.
`
}


func watch(glob string) {
	c := client.New("<test>", *addr)

	w, err := c.Watch(glob)
	if err != nil {
		bail(err)
	}

	for ev := range w.C {
		if ev.Err != nil {
			fmt.Fprintln(os.Stderr, ev.Err)
		}

		fmt.Println(ev.Path, ev.Cas, len(ev.Body))
		os.Stdout.Write(ev.Body)
		fmt.Println()
	}
}
