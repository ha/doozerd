include ../../Make.inc

TARG=doozer/peer
GOFILES=\
	peer.go\
	liveness.go\
	version.go\

include $(GOROOT)/src/Make.pkg

VERS ?= $(shell ./version.sh)
GVERS := $(shell sed 's/[^`]*`//' version.go 2>/dev/null | sed 's/`.*//')
version.go:
	printf 'package peer;const Version = `%s`\n' '$(VERS)' > $@

ifneq ($(VERS),$(GVERS))
.PHONY: version.go
endif

CLEANFILES+=version.go

mem.prof:
	make clean
	GOGC=off gotest\
		-file prof.go\
		-file misc_test.go\
		-memprofile $@\
		-memprofilerate 1\

cpu.prof:
	make clean
	GOGC=off gotest\
		-file prof.go\
		-file misc_test.go\
		-cpuprofile $@\
