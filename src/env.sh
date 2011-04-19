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
    quiet
    store
    consensus
    proto
    server
    web
    test
    member
    gc
    .
"

CMDS="
    doozerd
"
