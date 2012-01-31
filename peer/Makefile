.PHONY: install build test

build install test: version.go
	go $@

VERS ?= $(shell ./version.sh)
GVERS := $(shell sed 's/[^`]*`//' version.go 2>/dev/null | sed 's/`.*//')
version.go:
	printf 'package peer;const Version = `%s`\n' '$(VERS)' > $@

clean:
	rm -f version.go
