package shell

import (
	"os"
	"strings"
	"fmt"
)

type Reader interface {
	ReadString(delim byte) (string, os.Error)
}

type Writer interface {
	WriteString(string) (int, os.Error)
	Flush() os.Error
}

type UnknownCommandError struct {
	Cmd string
}

func (e UnknownCommandError) String() string {
	return fmt.Sprintf("unknown command: %s", e.Cmd)
}

type InvalidArgumentsError struct {
	Needed uint8
}

func (e InvalidArgumentsError) String() string {
	return fmt.Sprintf("arguments missing; %d needed", e.Needed)
}

func checkArgs(argv []string, n uint8) {
	if uint8(len(argv)) - 1 < n {
		panic(InvalidArgumentsError{n})
	}
}

func Exec(in Reader, out Writer) {

	store := make(map[string]string)

	main := func() {
		defer func() {
			error := recover()
			if error != nil {
				out.WriteString(error.(os.Error).String() + "\n")
				out.Flush()
			}
		}()

		out.WriteString(">> ")
		out.Flush()

		read, _ := in.ReadString('\n')
		read = strings.TrimSpace(read)
		argv := strings.Split(read, " ", 3)

		switch argv[0] {
		default:
			panic(UnknownCommandError{argv[0]})
		case "":
			// Do Nothing
		case "exit":
			os.Exit(0)
		case "set":
			checkArgs(argv, 2)
			store[argv[1]] = argv[2]
		case "get":
			checkArgs(argv, 1)
			if val, ok := store[argv[1]]; ok {
				out.WriteString(val + "\n")
			} else {
				out.WriteString("(nil)\n")
			}
		case "del":
			checkArgs(argv, 1)
			store[argv[1]] = "", false
		}
	}

	for {
		main()
	}
}
