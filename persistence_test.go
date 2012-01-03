package persistence

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestStore(t *testing.T) {
	f, err := ioutil.TempFile("", "journal")
	if err != nil {
		t.Log(err)
	}
	defer f.Close()
	name := f.Name()
	defer os.Remove(name)
	
	j, err := NewJournal(name)
	if err != nil {
		t.Log(err)
	}
	j.Close()
}
