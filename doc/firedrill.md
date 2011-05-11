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

        $ doozerd -l 127.0.0.1:8046 2>/dev/null &
        $ doozerd -l 127.0.0.2:8046 -a 127.0.0.1:8046 2>/dev/null &
        $ doozerd -l 127.0.0.3:8046 -a 127.0.0.1:8046 2>/dev/null &

2. Start a client:

        $ doozer watch '/**'

3. Kill one of the doozerds:

        $ kill doozerd

    Notice that activity continues in the cluster. The client
    continues printing updates.

4. Start a fourth

        $ doozerd -l 127.0.0.4:8046 -a 127.0.0.1:8046 2>/dev/null &
