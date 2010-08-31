package assert
// Testing helpers for junta.

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
		Equal(t, p, recover(), "panic test")
	}()
	f()
}
