#!/bin/bash

# doozer cluster initialization script
#
# this works by identifying a /ctl/cal/<id> for an instance of doozer
# on a host and performing an idempotent set of operations to ensure 
# that each note participating in a cluster is correctly bootstrapped.

set -o pipefail

function log() {
    echo "[`date +'%Y%m%d %H:%M:%S'`] $@"
}

if [ -z $HOSTNAME ]; then
    HOSTNAME=`hostname`
fi
CALID=""
DOOZERCLI="/usr/local/bin/doozer"
DOOZERD_NODES=""
HOST="127.0.0.1"
PORT="9200"

while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`
    case $PARAM in
        --cal-id)
            CALID="$VALUE"
            ;;
        --doozer-cli)
            DOOZERCLI="$VALUE"
            ;;
        --doozerd-nodes)
            DOOZERD_NODES="$VALUE"
            ;;
        --port)
            PORT="$VALUE"
            ;;
        --host)
            HOST="$VALUE"
            ;;
    esac
    shift
done

if [ -z $PORT ]; then
    log "ERROR: --port cannot be empty"
    exit 1
fi

if [ -z $HOST ]; then
    log "ERROR: --host cannot be empty"
    exit 1
fi

if [ -z $DOOZERD_NODES ]; then
    log "ERROR: --doozerd-nodes cannot be empty"
    exit 1
fi

log "DOOZERD_NODES: $DOOZERD_NODES"

# first identify which node to bind to (ie. the root node)
bind_node=""
for doozerd_node in $DOOZERD_NODES; do
    doozerd_node_hostname=`echo $doozerd_node | awk -F: '{print $1}'`
    doozerd_node_port=`echo $doozerd_node | awk -F: '{print $2}'`
    
    # resolve the ip (doozerd needs the exact ip to bind to, does not work with 0.0.0.0)
    doozerd_node_ip=`host $doozerd_node_hostname 2>/dev/null | tail -1 | awk '{print $NF}'`
    if $DOOZERCLI -a="doozer:?ca=$doozerd_node_ip:$doozerd_node_port" nop; then
        bind_node="$doozerd_node_ip:$doozerd_node_port"
        break
    fi
done

if [ -z $bind_node ]; then
    log "ERROR: could not find node to bind to"
    exit 1
fi

log "bind_node: $bind_node"

this_node_ip=`host $HOSTNAME 2>/dev/null | tail -1 | awk '{print $NF}'`
host_node="$this_node_ip:$PORT"

log "host_node: $host_node"

my_self=`$DOOZERCLI -a="doozer:?ca=$host_node" self`
if [ -z $my_self ]; then
    log "ERROR: could not identify self"
    exit 1
fi

log "my_self: $my_self"

if [ -z $CALID ]; then
    # calculate the CALID if the HOSTNAME is in the format name##.hostname.org
    CALID=$(printf %d $(expr $(echo $HOSTNAME | awk -F. '{print $1}' | tail -c -3) - 1))
fi

if [ -z $CALID ]; then
    log "ERROR: could not identify cal"
    exit 1
fi

log "CALID: $CALID"

log "checking /ctl/cal/$CALID"
stat_output=`$DOOZERCLI -a="doozer:?ca=$bind_node" stat /ctl/cal/$CALID 2>/dev/null`
if [ "$?" == "0" ]; then
    log "/ctl/cal/$CALID exists"
    cal_self=`$DOOZERCLI -a="doozer:?ca=$bind_node" get /ctl/cal/$CALID 2>/dev/null`
    if [ "$cal_self" == "$my_self" ]; then
        log "NOTICE: nothing to do (we're already active in the cluster)"
        exit 0
    fi
    rev=`echo $stat_output | awk '{print $1}'`
    log "deleting /ctl/cal/$CALID @ $rev"
    $DOOZERCLI -a="doozer:?ca=$bind_node" del /ctl/cal/$CALID $rev >/dev/null 2>&1
    if [ "$?" != "0" ]; then
        log "ERROR: failed to delete /ctl/cal/$CALID"
        exit 1
    fi
fi

log "adding /ctl/cal/$CALID"
echo -n | $DOOZERCLI -a="doozer:?ca=$bind_node" add /ctl/cal/$CALID
if [ "$?" != "0" ]; then
    log "ERROR: failed to add /ctl/cal/$CALID"
    exit 1
fi

log "SUCCESS"
