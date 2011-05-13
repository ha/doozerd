# Fire Drills

Experience doozer's fault tolerance firsthand.

This document intends to give examples of failure
scenarios and how you might deal with them. Our hope
is that it will evolve into a useful playbook for
handling actual failures.

## Node Failure

The plan: boot three consensors, kill one, then replace it with
a fresh fourth. Clients will see no interruption in service.

1. Start three active doozerds:

        $ doozerd -timeout 5 -l 127.0.0.1:8046 -w 127.0.0.1:8000 2>/dev/null &
        $ doozerd -timeout 5 -l 127.0.0.1:8047 -w 127.0.0.1:8001 -a 127.0.0.1:8046 2>/dev/null &
        $ doozerd -timeout 5 -l 127.0.0.1:8048 -w 127.0.0.1:8002 -a 127.0.0.1:8046 2>/dev/null &
        $ echo -n | doozer add /ctl/cal/1 # activate the second one
        $ echo -n | doozer add /ctl/cal/2 # activate the third one

2. Start a client:

        $ doozer watch '/**'

3. Kill one of the doozerds:

        $ kill -STOP `ps auxww|grep doozerd|grep :8047|awk '{print $2}'`

    Notice that activity continues in the cluster. The client
    continues printing updates, but with NOPs in place of changes
    that would have been originated by the dead node. After about
    five seconds (the value you gave for -timeout) the dead node
    will be removed by the other two and the cluster will shrink
    from three to two nodes.

    (Note: the command above kills the doozerd running on port 8047.
    You could kill any of the three doozerds, but the client doesn't
    yet try to reconnect if it loses its connection, so it's easier
    to demonstrate doozerd's behavior by killing one the client isn't
    connected to.)

4. Start a fourth

        $ doozerd -timeout 5 -l 127.0.0.1:8049 -w 127.0.0.1:8004 -a 127.0.0.1:8046 2>/dev/null &

5. Cleanup

        $ killall -9 doozerd doozer
