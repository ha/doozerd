include ../../Make.inc

TARG=doozer/consensus
GOFILES=\
	consensus.go\
	run.go\
	coordinator.go\
	acceptor.go\
	learner.go\
	m.go\
	m.pb.go\
	manager.go\

include $(GOROOT)/src/Make.pkg

.PHONY: m.pb.go
m.pb.go: m.proto
	mkdir -p _pb
	protoc --go_out=_pb $<
	cat _pb/$@\
	|sed s/Msg/msg/g\
	|sed s/Newmsg/newMsg/g\
	|gofmt >$@
	rm -rf _pb

CLEANFILES+=_pb
