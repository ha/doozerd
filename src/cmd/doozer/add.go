package main


func init() {
	cmds["add"] = cmd{add, "<path>", "add a key only if it isn't already set"}
	cmdHelp["add"] = `Sets the body of the file located at <path> only if it didn't already exist.

Equivalent to

  set <path> 0

The body is read from stdin. If <cas> does not match the existing CAS token of
the file, no change will be made.

Prints the new CAS token on stdout, or an error message on stderr.
`
}


func add(path string) {
	set(path, "0")
}
