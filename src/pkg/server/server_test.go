package server

import (
	"bytes"
	"github.com/bmizerany/assert"
	"goprotobuf.googlecode.com/hg/proto"
	"testing"
)


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
	}
	tx := &txn{
		c:   c,
		req: request{Tag: proto.Int32(1)},
	}
	tx.set()
	assertResponseErrCode(t, response_MISSING_ARG, c)
}
