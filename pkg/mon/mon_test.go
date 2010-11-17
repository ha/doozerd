package mon

import (
	"doozer/assert"
	"testing"
)

func TestSplitIdSimple(t *testing.T) {
	name, ext := splitId("a.service")
	assert.Equal(t, "a", name)
	assert.Equal(t, ".service", ext)
}

func TestSplitIdWithDots(t *testing.T) {
	name, ext := splitId("foo.heroku.com.service")
	assert.Equal(t, "foo.heroku.com", name)
	assert.Equal(t, ".service", ext)
}
