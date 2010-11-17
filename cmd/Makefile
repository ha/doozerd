all: install

DIRS=\
     doozerd\

%.install:
	+cd $* && gomake install

install: $(addsuffix .install, $(DIRS))

%.clean:
	+cd $* && gomake clean

clean: $(addsuffix .clean, $(DIRS))
