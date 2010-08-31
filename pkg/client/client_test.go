package client

import (
	"testing"
	"bytes"
	"bufio"
	"os"

	"junta/assert"
	"junta/proto"
)

// TESTING
//
// TODO:  Better tests please.  As of the time of writing,
// we're still on the fence about the API, so we're only
// testing low-hanging fruit.

func TestClientSet(t *testing.T) {
	resp := new(bytes.Buffer)
	proto.Encode(resp, "123")

	r := bufio.NewReader(resp)
	w := bufio.NewWriter(new(bytes.Buffer))
	client := &Client{r, w}

	err := client.Set("foo", "bar")

	assert.True(t, err == nil)
}

func TestClientSetWithEOF(t *testing.T) {
	resp := bytes.NewBufferString("*1\rASDF")

	r := bufio.NewReader(resp)
	w := bufio.NewWriter(new(bytes.Buffer))
	client := &Client{r, w}

	err := client.Set("foo", "bar")

	assert.Equal(t, os.EOF, err, "")
}

func TestClientGetSimple(t *testing.T) {
	resp := new(bytes.Buffer)
	proto.Encode(resp, "bar")

	r := bufio.NewReader(resp)
	w := bufio.NewWriter(new(bytes.Buffer))
	client := &Client{r, w}

	body, err := client.Get("foo")

	assert.True(t, err == nil)
	assert.Equal(t, "bar", body, "")
}

func TestClientGetWithEOF(t *testing.T) {
	resp := bytes.NewBufferString("*1\rASDF")

	r := bufio.NewReader(resp)
	w := bufio.NewWriter(new(bytes.Buffer))
	client := &Client{r, w}

	body, err := client.Get("foo")

	assert.Equal(t, os.EOF, err, "")
	assert.Equal(t, "", body, "")
}
