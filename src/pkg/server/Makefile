include ../../Make.inc

TARG=doozer/server
GOFILES=\
	msg.pb.go\
	server.go\
	txn.go\

include $(GOROOT)/src/Make.pkg

msg.pb.go: msg.proto
	mkdir -p _pb
	protoc --go_out=_pb $<
	cat _pb/$@\
	|sed s/\\bRequest/request/g\
	|sed s/\\bResponse/response/g\
	|sed s/\\bNewRequest/newRequest/g\
	|sed s/\\bNewResponse/newResponse/g\
	|gofmt >$@
	rm -rf _pb

CLEANFILES+=_pb
