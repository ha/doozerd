package persistence

import (
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

// Test data stolen from crypto/sha1/sha1_test.go.
var testData = []string{
	"",
	"a",
	"ab",
	"abc",
	"abcd",
	"abcde",
	"abcdef",
	"abcdefg",
	"abcdefgh",
	"abcdefghi",
	"abcdefghij",
	"Discard medicine more than two years old.",
	"He who has a shady past knows that nice guys finish last.",
	"I wouldn't marry him with a ten foot pole.",
	"Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave",
	"The days of the digital watch are numbered.  -Tom Stoppard",
	"Nepal premier won't resign.",
	"For every action there is an equal and opposite government program.",
	"His money is twice tainted: 'taint yours and 'taint mine.",
	"There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977",
	"It's a tiny change to the code and not completely disgusting. - Bob Manchek",
	"size:  a.out:  bad magic",
	"The major problem is with sendmail.  -Mark Horton",
	"Give me a rock, paper and scissors and I will move the world.  CCFestoon",
	"If the enemy is within range, then so are you.",
	"It's well we cannot hear the screams/That we create in others' dreams.",
	"You remind me of a TV show, but that's all right: I watch it anyway.",
	"C is as portable as Stonehedge!!",
	"Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley",
	"The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule",
	"How can you write a big system without C++?  -Paul Glick",
}

var testFileSha1 = "2c9e98acf63756007d3e3bbdcc4d80882e06a2aa"

func TestStore(t *testing.T) {
	f, err := ioutil.TempFile("", "journal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	name := f.Name()

	j, err := NewJournal(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range testData {
		err = j.Store(v)
		if err != nil {
			t.Fatal(err)
		}
	}
	j.Close()
	
	b, err := ioutil.ReadFile(name)
	if (err != nil) {
		t.Fatal(err)
	}
	sha1 := sha1.New()
	sha1.Write(b)
	s := fmt.Sprintf("%x", sha1.Sum(nil))
	if s != testFileSha1 {
		t.Fatal("journal file has an unexpected SHA-1")
	}
	
	os.Remove(name)
}

func TestRetrieve(t *testing.T) {
	f, err := ioutil.TempFile("", "journal")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	name := f.Name()
	defer os.Remove(name)

	// Generate some random data.
	randData := make([]string, 128)
	r := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	for i, _ := range randData {
		tmp := make([]byte, r.Intn(2<<16))
		for j := range tmp {
			tmp[j] = byte(r.Intn(256))
		}
		randData[i] = string(tmp)
	}
	// Append it to the static data.
	testData = append(testData, randData...)

	j, err := NewJournal(name)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range testData {
		err = j.Store(v)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range testData {
		m, err := j.Retrieve()
		if err != nil {
			t.Fatal(err)
		}
		if m != v {
			t.Fatalf("read from store '%v', should be '%v'", m, v)
		}
	}
	j.Close()
}
