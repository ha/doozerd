package assert

import (
	"testing"
)

func TestLineNumbers(t *testing.T) {
	Equal(t, "foo", "bar", "msg!")
}
