package exec

import (
	"junta/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestTrue(t *testing.T) {
	cx := Context{}
	pid, err := cx.ForkExec("/bin/true", nil)
	assert.Equal(t, nil, err)
	assert.T(t, pid > 0)
	w, err := os.Wait(pid, 0)
	assert.Equal(t, true, w.Exited())
	assert.Equal(t, 0, w.ExitStatus())
}

func TestFalse(t *testing.T) {
	cx := Context{}
	pid, err := cx.ForkExec("/bin/false", nil)
	assert.Equal(t, nil, err)
	assert.T(t, pid > 0)
	w, err := os.Wait(pid, 0)
	assert.Equal(t, true, w.Exited())
	assert.NotEqual(t, 0, w.ExitStatus())
}

func TestLF(t *testing.T) {
	cx := Context{}
	pr, pw, err := os.Pipe()
	lf := []*os.File{pw}

	pid, err := cx.ForkExec("./test-lf.sh", lf)
	assert.Equal(t, nil, err)
	assert.T(t, pid > 0)

	pw.Close()

	line, err := ioutil.ReadAll(pr)
	assert.Equal(t, nil, err)
	assert.Equal(t, []byte{'a'}, line)

	w, err := os.Wait(pid, 0)
	assert.Equal(t, true, w.Exited())
	assert.Equal(t, 0, w.ExitStatus())
}
