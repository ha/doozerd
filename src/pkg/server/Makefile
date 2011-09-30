include ../../Make.inc

TARG=doozer/server
GOFILES=\
	conn.go\
	msg.pb.go\
	server.go\
	txn.go\

include $(GOROOT)/src/Make.pkg

msg.pb.go: msg.proto
	mkdir -p _pb
	protoc --go_out=_pb $<
	cat _pb/$@\
	|sed s/Request/request/g\
	|sed s/Response/response/g\
	|sed s/Newrequest/newRequest/g\
	|sed s/Newresponse/newResponse/g\
	|gofmt >$@
	rm -rf _pb

CLEANFILES+=_pb
