package assert
// Testing helpers for junta.

import (
	"reflect"
	"testing"
	"runtime"
)

func equal(t *testing.T, expected, result interface{}, message string, cd int) {
		_, file, line, _ := runtime.Caller(cd)
		if !reflect.DeepEqual(expected, result) {
			t.Errorf("%s:%d: (%s)", file, line, message)
			t.Errorf("    expected: <%#v>", expected)
			t.Errorf("    but got:  <%#v>", result)
			t.FailNow()
		}
}

func Equal(t *testing.T, expected, result interface{}, message string) {
	equal(t, expected, result, message, 2)
}

func Panic(t *testing.T, p interface {},  f func()) {
	defer func() {
		equal(t, p, recover(), "panic test", 3)
	}()
	f()
}
