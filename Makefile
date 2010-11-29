include $(GOROOT)/src/Make.inc

all: install
install: install.cmd
clean: clean.cmd clean.pkg-inst clean.doozerd-inst
test: test.pkg
bench: bench.pkg

%.cmd: %.pkg
	cd cmd && make $*

%.pkg:
	cd pkg/util && make $*
	cd pkg/exec && make $*
	cd pkg/store && make $*
	cd pkg/paxos && make $*
	cd pkg/net && make $*
	cd pkg/proto && make $*
	cd pkg/lock && make $*
	cd pkg/server && make $*
	cd pkg/web && make $*
	cd pkg/client && make $*
	cd pkg/mon && make $*
	cd pkg/test && make $*
	cd pkg/timer && make $*
	cd pkg/session && make $*
	cd pkg/member && make $*
	cd pkg/gc && make $*
	cd pkg && make $*

.PHONY: clean.pkg-inst
clean.pkg-inst:
	rm -rf $(GOROOT)/pkg/$(GOOS)_$(GOARCH)/doozer
	rm -rf $(GOROOT)/pkg/$(GOOS)_$(GOARCH)/doozer.a

.PHONY: clean.doozerd-inst
clean.doozerd-inst:
	rm -rf $(GOBIN)/doozerd
