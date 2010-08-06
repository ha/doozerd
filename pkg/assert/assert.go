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

