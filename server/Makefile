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
