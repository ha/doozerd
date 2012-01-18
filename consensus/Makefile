.PHONY: install build test

build install test : m.pb.go
	go $@

m.pb.go: m.proto
	mkdir -p _pb
	protoc --go_out=_pb $<
	cat _pb/$@\
	|sed s/Msg/msg/g\
	|sed s/Newmsg/newMsg/g\
	|gofmt >$@
	rm -rf _pb
