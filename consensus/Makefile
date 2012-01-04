include ../../Make.inc

TARG=github.com/ha/doozerd/consensus
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

m.pb.go: m.proto
	mkdir -p _pb
	protoc --go_out=_pb $<
	cat _pb/$@\
	|sed s/Msg/msg/g\
	|sed s/Newmsg/newMsg/g\
	|gofmt >$@
	rm -rf _pb

CLEANFILES+=_pb
