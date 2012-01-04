VERS ?= $(shell ./version.sh)
GVERS := $(shell sed 's/[^`]*`//' version.go 2>/dev/null | sed 's/`.*//')
version.go:
	printf 'package peer;const Version = `%s`\n' '$(VERS)' > $@
