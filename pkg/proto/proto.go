package proto

import (
	"bufio"
	"strconv"
	"strings"
	"os"
)

type Request struct {
	Parts [][]byte
	Err   os.Error
}

var (
	ProtocolError = os.NewError("multi bulk protocol error")
)

func scanNumber(data *bufio.Reader, after byte) (n uint64, err os.Error) {

	for {
		var c byte

		c, err = data.ReadByte()
		if err != nil {
			return
		}

		switch c {
		default:
			err = ProtocolError
			return
		case '\r', '\n':
			continue
		case after:
			var sn string
			sn, err = data.ReadString('\n')
			if err != nil {
				return
			}

			return strconv.Btoui64(strings.TrimSpace(sn), 10)
		}
	}

	panic("This should never be reached!")
}

func Scan(data *bufio.Reader, ch chan *Request) {

	for {
		count, err := scanNumber(data, '*')
		if err != nil {
			ch <- &Request{Err: err}
		}

		parts := make([][]byte, count)

		for count > 0 {
			size, err := scanNumber(data, '$')
			if err != nil {
				ch <- &Request{Err: err}
			}

			// Read the data
			bytes := make([]byte, size)
			_, err = data.Read(bytes)
			if err != nil {
				ch <- &Request{Err: err}
			}

			parts[len(parts)-int(count)] = bytes

			count--
		}

		ch <- &Request{Parts: parts}
	}

}
