# figure out what GOROOT is supposed to be
GOROOT ?= $(shell printf 't:;@echo $$(GOROOT)\n' | gomake -f -)
include $(GOROOT)/src/Make.inc

TARG=persistence
GOFILES=\
	arena.go\
	block.go\
	persistence.go\

include $(GOROOT)/src/Make.pkg
