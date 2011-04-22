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

PKGS="
    quiet
    store
    consensus
    server
    web
    test
    member
    gc
    peer
"

CMDS="
    doozerd
"
