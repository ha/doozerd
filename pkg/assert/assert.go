package assert
// Testing helpers for borg.

import (
	"reflect"
	"testing"
)

func Equal(t *testing.T, expected, result interface{}, message string) {
	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("expected <%#v> but got <%#v> (%s)", expected, result, message)
	}
}

func Panic(t *testing.T, p interface {},  f func()) {
	defer func() {
		e := recover()
		if e == nil {
			t.Fatal("expected panic but did not\n")
		} else {
			Equal(t, p, e, "panic test")
		}
	}()
	f()
}
