package server

import (
	"github.com/bmizerany/assert"
	"github.com/kr/pretty"
	"testing"
)

type grantTest struct {
	c  *conn
	sk string
	r  bool
	w  bool
	ok bool
}

var grantTests = []grantTest{
	// same
	{&conn{rosk: "p", rwsk: "p"}, "x", false, false, false},
	{&conn{rosk: "p", rwsk: "p"}, "p", true, true, true},

	// different
	{&conn{rosk: "a", rwsk: "b"}, "x", false, false, false},
	{&conn{rosk: "a", rwsk: "b"}, "a", true, false, true},
	{&conn{rosk: "a", rwsk: "b"}, "b", true, true, true},

	// test blank passwords explicitly; the rules are
	// the same as above, but this is a common case
	{&conn{rosk: "", rwsk: ""}, "", true, true, true},
	{&conn{rosk: "", rwsk: "b"}, "", true, false, true},
	{&conn{rosk: "", rwsk: "b"}, "b", true, true, true},
}

func TestConnGrant(t *testing.T) {
	for _, tst := range grantTests {
		ok := tst.c.grant(tst.sk)
		assert.Equalf(t, tst.ok, ok, "%# v", pretty.Formatter(tst))
		assert.Equalf(t, tst.r, tst.c.raccess, "%# v", pretty.Formatter(tst))
		assert.Equalf(t, tst.w, tst.c.waccess, "%# v", pretty.Formatter(tst))
	}
}
