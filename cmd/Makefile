all: install

DIRS=\
     juntad\

%.install:
	+cd $* && gomake install

install: $(addsuffix .install, $(DIRS))
