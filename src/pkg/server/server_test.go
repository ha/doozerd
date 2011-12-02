package server

import (
	"bytes"
	"doozer/store"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"io"

	"testing"
)

var (
	fooPath = "/foo"
)

type bchan chan []byte

func (b bchan) Write(buf []byte) (int, error) {
	b <- buf
	return len(buf), nil
}

func (b bchan) Read(buf []byte) (int, error) {
	return 0, io.EOF // not implemented
}

func mustUnmarshal(b []byte) (r *response) {
	r = new(response)
	err := proto.Unmarshal(b, r)
	if err != nil {
		panic(err)
	}
	return
}

func assertResponseErrCode(t *testing.T, exp response_Err, c *conn) {
	b := c.c.(*bytes.Buffer).Bytes()
	assert.T(t, len(b) > 4, b)
	assert.Equal(t, &exp, mustUnmarshal(b[4:]).ErrCode)
}

func TestDelNilFields(t *testing.T) {
	c := &conn{
		c:        &bytes.Buffer{},
		canWrite: true,
		waccess:  true,
	}
	tx := &txn{
		c:   c,
		req: request{Tag: proto.Int32(1)},
	}
	tx.del()
	assertResponseErrCode(t, response_MISSING_ARG, c)
}

func TestSetNilFields(t *testing.T) {
	c := &conn{
		c:        &bytes.Buffer{},
		canWrite: true,
		waccess:  true,
	}
	tx := &txn{
		c:   c,
		req: request{Tag: proto.Int32(1)},
	}
	tx.set()
	assertResponseErrCode(t, response_MISSING_ARG, c)
}

func TestServerNoAccess(t *testing.T) {
	b := make(bchan, 2)
	c := &conn{
		c:        b,
		canWrite: true,
		st:       store.New(),
	}
	tx := &txn{
		c:   c,
		req: request{Tag: proto.Int32(1)},
	}

	for i, op := range ops {
		if i != int32(request_ACCESS) {
			op(tx)
			var exp response_Err = response_OTHER
			assert.Equal(t, 4, len(<-b), request_Verb_name[i])
			assert.Equal(t, &exp, mustUnmarshal(<-b).ErrCode, request_Verb_name[i])
		}
	}
}

func TestServerRo(t *testing.T) {
	b := make(bchan, 2)
	c := &conn{
		c:        b,
		canWrite: true,
		st:       store.New(),
	}
	tx := &txn{
		c:   c,
		req: request{Tag: proto.Int32(1)},
	}

	wops := []int32{int32(request_DEL), int32(request_NOP), int32(request_SET)}

	for _, i := range wops {
		op := ops[i]
		op(tx)
		var exp response_Err = response_OTHER
		assert.Equal(t, 4, len(<-b), request_Verb_name[i])
		assert.Equal(t, &exp, mustUnmarshal(<-b).ErrCode, request_Verb_name[i])
	}
}
