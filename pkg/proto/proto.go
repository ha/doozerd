package proto

import (
	"os"
	"io"
	"fmt"
	"strings"
	"strconv"
)

type ReadStringer interface {
	io.Reader
	ReadString(byte) (string, os.Error)
}

func Decode(r ReadStringer) (parts []string, err os.Error) {
	var count int = 1
	var size int
	var line string

Loop:
	for count > 0 {
		// TODO: test if len(line) == 0
		line, err = r.ReadString('\n')
		switch {
		case err == os.EOF: break Loop
		case err != nil: panic(err)
		}
		line = strings.TrimSpace(line)
		if len(line) < 1 {
			continue Loop
		}
		switch line[0] {
		case '*':
			count, _ = strconv.Atoi(line[1:])
			parts = make([]string, count)
		case '$':
			// TODO: test for err
			size, _ = strconv.Atoi(line[1:])
			buf := make([]byte, size)
			// TODO: test for err
			n, err := io.ReadFull(r, buf)
			switch {
			case n != size: panic(fmt.Sprintf("n:%d\n", n))
			case err != nil: panic(err)
			}
			parts[len(parts) - count] = string(buf)
			count--
		}
	}
	return
}

func Encode(w io.Writer, parts ... string) (err os.Error) {
	_, err = fmt.Fprintf(w, "*%d\r\n", len(parts))
	for _, part := range parts {
		_, err = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(part), part)
	}
	return err
}
