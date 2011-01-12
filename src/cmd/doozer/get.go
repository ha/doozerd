package main

import (
	"doozer/client"
	"fmt"
    "os"
)


func init() {
	cmds["get"] = cmd{get, "<path>", "read a file"}
	cmdHelp["get"] = `Prints the CAS token and body of the file at <path>.

Output format:

  <cas> <len> LF <body> LF

Here, <cas> is the cas token, <len> is the number of bytes in the body,
<body> is the bytes of the body, and LF is an ASCII line-feed char.
`
}


func get(path string) {
	c := client.New("<test>", *addr)

	body, cas, err := c.Get(path, 0)
	if err != nil {
		bail(err)
	}

	fmt.Println(cas, len(body))
    os.Stdout.Write(body)
	fmt.Println()
}
