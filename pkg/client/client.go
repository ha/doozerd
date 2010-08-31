package client

import (
	"io"
	"os"
	"junta/proto"
)

type Client struct {
	r proto.ReadStringer
	w io.Writer
}

// Errors that can happen
var (
	InvalidResponse = os.NewError("Invalid Response from server.")
)

func (c *Client) Set(path, body string) os.Error {
	err := proto.Encode(c.w, "set", path, body)
	if err != nil {
		return err
	}
	_, err = proto.Decode(c.r)
	return err
}

func (c *Client) Get(path string) (string, os.Error) {
	err := proto.Encode(c.w, "get", path)
	if err != nil {
		return "", err
	}
	parts, err := proto.Decode(c.r)
	if len(parts) > 0 {
		return parts[0], err
	}
	return "", err
}

