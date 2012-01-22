package main

import (
	"flag"
	"fmt"
	"github.com/ha/doozerd/persistence"
	"io"
	"os"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: dumpjournal file")
	os.Exit(1)
}

func main() {
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		usage()
	}
	file := args[0]
	j, err := persistence.NewJournal(file)
	if err != nil {
		panic(err)
	}
	for {
		m, err := j.ReadMutation()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return
		}
		fmt.Println(m)
	}
}
