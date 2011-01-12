package main

import (
	"doozer/client"
	"fmt"
    "os"
)


func init() {
	cmds["walk"] = cmd{walk, "<glob>", "read many files"}
	cmdHelp["walk"] = `Prints the path, CAS token, and body of each file matching <glob>.

Prints a sequence of records, one for each file. Format of each record:

  <path> <cas> <len> LF <body> LF

Here, <path> is the file's path, <cas> is the cas token, <len> is the number of
bytes in the body, <body> is the bytes of the body, and LF is an ASCII
line-feed char.
`
}


func walk(glob string) {
	c := client.New("<test>", *addr)

	w, err := c.Walk(glob)
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
