if test -z "$GOROOT"
then
    # figure out what GOROOT is supposed to be
    GOROOT=`printf 't:;@echo $(GOROOT)\n' | gomake -f -`
    export GOROOT
fi

PKG_REQS="
    goprotobuf.googlecode.com/hg/proto
    github.com/bmizerany/assert
"

CMD_REQS="
    $GOROOT/src/pkg/goprotobuf.googlecode.com/hg/compiler
"

PKGS="
    util
    store
    ack
    consensus
    proto
    lock
    server
    web
    client
    test
    timer
    session
    member
    gc
    .
"

CMDS="
    doozerd
    doozer
"
