all: install.cmd
clean: clean.cmd
test: test.pkg
bench: bench.pkg

%.cmd: %.pkg
	cd cmd && make $*

%.pkg:
	cd pkg && make $*

