package borg

import (
  "testing"
)

func TestConst(t *testing.T) {
  exp, got := 7, Foo()
  if got != exp {
      t.Errorf("expected %d, got %#v\n", exp, got)
  }
}
