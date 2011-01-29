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
    paxos
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
