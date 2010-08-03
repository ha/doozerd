all: install

DIRS=\
     borgd\

%.install:
	+cd $* && gomake install

install: $(addsuffix .install, $(DIRS))
