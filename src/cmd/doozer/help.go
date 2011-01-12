package main

import "fmt"


func init() {
	cmds["help"] = cmd{help, "provide detailed help on a command"}
	cmdHelp["help"] = `help [options] <command>
`
}


func help(command string) {
	fmt.Printf("Use: %s [options] ", self)
	fmt.Print(cmdHelp[command])
}
