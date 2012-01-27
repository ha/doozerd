package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/ha/doozerd/persistence"
	"os"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: writejournal file")
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
	r := bufio.NewReader(os.Stdin)
	for {
		line, prefix, err := r.ReadLine()
		if err != nil {
			return
		}
		for prefix {
			var l []byte
			l, prefix, _ = r.ReadLine()
			if err != nil {
				return
			}
			line = append(line, l...)
		}
		err = j.WriteMutation(string(line))
		if err != nil {
			panic(err)
		}
	}
}
