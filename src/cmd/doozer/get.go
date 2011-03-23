package main

import (
	"doozer/client"
	"fmt"
	"os"
)


func init() {
	cmds["get"] = cmd{get, "<path>", "read a file"}
	cmdHelp["get"] = `Prints the revision and body of the file at <path>.

Output format:

  <rev> <len> LF <body> LF

Here, <rev> is the revision, <len> is the number of bytes in the body,
<body> is the bytes of the body, and LF is an ASCII line-feed char.
`
}


func get(path string) {
	c := client.New("<test>", *addr)

	body, rev, err := c.Get(path, nil)
	if err != nil {
		bail(err)
	}

	fmt.Println(rev, len(body))
	os.Stdout.Write(body)
	fmt.Println()
}
