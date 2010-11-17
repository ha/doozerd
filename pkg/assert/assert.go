package assert
// Testing helpers for doozer.

import (
	"reflect"
	"testing"
	"runtime"
	"fmt"
)

func assert(t *testing.T, b bool, f func(), cd int) {
	if !b {
		_, file, line, _ := runtime.Caller(cd + 1)
		t.Errorf("%s:%d", file, line)
		f()
		t.FailNow()
	}
}

func equal(t *testing.T, exp, got interface{}, cd int, args ...interface{}) {
	f := func() {
		t.Errorf("!  Expected: %T %#v", exp, exp)
		t.Errorf("!  Got:      %T %#v", got, got)
		if len(args) > 0 {
			t.Error("!", " -", fmt.Sprint(args...))
		}
	}
	b := reflect.DeepEqual(exp, got)
	assert(t, b, f, cd+1)
}

func tt(t *testing.T, b bool, cd int, args ...interface{}) {
	f := func() {
		t.Errorf("!  Failure")
		if len(args) > 0 {
			t.Error("!", " -", fmt.Sprint(args...))
		}
	}
	assert(t, b, f, cd+1)
}

func T(t *testing.T, b bool, args ...interface{}) {
	tt(t, b, 1, args...)
}

func Tf(t *testing.T, b bool, format string, args ...interface{}) {
	tt(t, b, 1, fmt.Sprintf(format, args...))
}

func Equal(t *testing.T, exp, got interface{}, args ...interface{}) {
	equal(t, exp, got, 1, args...)
}

func Equalf(t *testing.T, exp, got interface{}, format string, args ...interface{}) {
	equal(t, exp, got, 1, fmt.Sprintf(format, args...))
}

func NotEqual(t *testing.T, exp, got interface{}, args ...interface{}) {
	f := func() {
		t.Errorf("!  Unexpected: <%#v>", exp)
		if len(args) > 0 {
			t.Error("!", " -", fmt.Sprint(args...))
		}
	}
	b := !reflect.DeepEqual(exp, got)
	assert(t, b, f, 1)
}

func Panic(t *testing.T, p interface{}, f func()) {
	defer func() {
		equal(t, p, recover(), 3)
	}()
	f()
}
