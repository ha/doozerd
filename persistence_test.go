package persistence

import (
	"io/ioutil"
//	"os"
	"testing"
)

func TestStore(t *testing.T) {
	f, err := ioutil.TempFile("", "journal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	name := f.Name()
//	defer os.Remove(name)
	
	j, err := NewJournal(name)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		err = j.Store("a")
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		_, err := j.Retrieve()
		if err != nil {
			t.Fatal(err)
		}
	}
	j.Close()
}
