# Clustering Doozer

Doozer is intended to be run as a cluster of 2 or more Doozer nodes over a
network.  Each Doozer in a cluster is either a CAL or a Slave.

**CAL**

CAL stands for Coordinator, Acceptor, Learner as defined by Paxos.  When a
Doozer is a CAL, it plays the role of all 3 in concert with other CALs in the
cluster to reach consensus on write operations.

**Slave**

A Doozer attached to another starts life as a read-only slave.  It records all
mutations to the store while watching for a new CAL slot to become
available.  In the event of an available CAL slot, a slave will attempt to
upgrade itself to CAL by attempting to lock that slot.

## Methods of clustering

### Without a Doozer name server

Start the initial Doozer.  This will start life as the only CAL in a cluster of
1.

    $ doozerd

Start the remaining 3 by attaching them to the first.  These can be brought up
simultaneously.

    $ doozerd -a 127.0.0.1:8046 -l 127.0.0.1:8047 -w :8001
    $ doozerd -a 127.0.0.1:8046 -l 127.0.0.1:8048 -w :8002
    $ doozerd -a 127.0.0.1:8046 -l 127.0.0.1:8049 -w :8003

Open http://127.0.0.1:8000 through 8003.  Notice the first Doozer we
started has its id set in `/ctl/cal/0`.  This means we only have one CAL.

**Adding CALs**

Adding more CALs is easy.  Simply create an empty file under `/ctl/cal/` and
watch the slaves attempt to lock it.  Only one will win.

    $ printf '' | doozer -a 'doozer:?ca=127.0.0.1:8046' set /ctl/cal/1 0
    $ printf '' | doozer -a 'doozer:?ca=127.0.0.1:8046' set /ctl/cal/2 0

Clients can now write to any of the Doozers who have their id set as the body of any
of the files listed under `/ctl/cal`.  If you attempt a write to the slave, you
will get a READ_ONLY error.

### With a Doozer name service

Managing multiple Doozer clusters is easier with a Doozer name service.  A
Doozer name service is an existing cluster of Doozer nodes with the intent of
being used to coordinate the creation of future, independent clusters.

Creating a name service is done as shown above.  Once it is running, you can
boot a new cluster without having to boot the inital CAL, wait for it to start,
then boot the rest.  Instead, you start all of the doozers in a cluster and let
them elect their own intial CAL using the name service.

An example using the cluster created above as the name service:

Create a variable to hold the URI pointing to the name service.

    $ DOOZER_BOOT_URI="doozer:?ca=127.0.0.1:8046&ca=127.0.0.1:8047&127.0.0.1:8048"

Bring up a cluster of 4, using the `-b` flag to tell a Doozer to use the name
service.  The `-c` sets the name of this new cluster to `ls`.

    $ doozerd -c ls -b "$DOOZER_BOOT_URI" -l 127.0.0.1:9046 -w :9000
    $ doozerd -c ls -b "$DOOZER_BOOT_URI" -l 127.0.0.1:9047 -w :9001
    $ doozerd -c ls -b "$DOOZER_BOOT_URI" -l 127.0.0.1:9048 -w :9002
    $ doozerd -c ls -b "$DOOZER_BOOT_URI" -l 127.0.0.1:9049 -w :9003


Open http://127.0.0.1:8000 to see the boot clusters files.

Open http://127.0.0.1:9000 to see your new cluster.

## How many should I have in a cluster?

It's generally good practice to have an odd number of CALs.  This means you
are less likely to end up with dueling-proposers.

The number of CALs in a cluster impacts performance because it takes
`(CALs/2)+1` to reach consensus.  This means more network traffic and more nodes
to wait for votes from.  On the other hand, more CALs in a cluster give the
cluster a greater tolerance to faults.

You will need to find the sweet spot for your enviroment and needs.
