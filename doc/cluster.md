# Clustering Doozer

## Overview

A Doozer *cluster* is one or more `doozerd` processes interconnected with one
another, where all the participants are performing consensus. Each member of a
cluster is referred to as a *cal*.  CAL stands for "Coordinator, Acceptor,
and Learner" as defined by Paxos.

CALs can propose changes to the state of the system (or *mutations*) through
consensus, vote for their proposals and/or others, and learn winning proposals.
For more information about this, see [Consensus][].

A `doozerd` process attached to a cluster that is not participating in
consensus is a *slave*. Slaves can only learn winning proposals and update their
state accordingly.  Meanwhile, they are watching for an available cal
slot to become available.  In the event of a cal slot becoming available, slaves
will attempt to lock the right to take over that responsibility. 

## Bootstrapping

For a cluster to start, it needs either an initial CAL process for others to
attach to, or a *Doozer Name Service*.

A `doozerd` started without being given another `doozerd` process to attach to,
will start life as the only CAL in a cluster of one.  A `doozerd` that is
attached to a cluster starts life as a slave.

Start the initial CAL:

    $ doozerd -l 127.0.0.1:8046

NOTE: Doozer uses the address given to `-l` as an identifier.  It is not
sufficient to use `0.0.0.0`.  It needs the address others will connect to it
with.

We can now slave our initial instance.

    $ doozerd -l 127.0.0.2:8064 -a 127.0.0.1:8046

Open http://127.0.0.2:8000 to view its web view.  Note it sees itself and the
initial CAL.

Add the third slave:


    $ doozerd -l 127.0.0.3:8064 -a 127.0.0.1:8046

Pro Tip:  Once the initial CAL is booted.  Slaves can connect at anytime,
meaning you can launch them in parallel.

## Adding CAL slots

We need to use a Doozer client to add CAL slots.  For this example we will use the `doozer` command.

Create two empty files under `/ctl/cal`.

    $ printf '' | doozer -a 'doozer?:ca=127.0.0.1:8046' set /ctl/cal/1 0
    $ printf '' | doozer -a 'doozer?:ca=127.0.0.1:8046' set /ctl/cal/2 0

NOTES: 

  * We're using a [Doozer URI][uri] for `-a`).

  * This is a normal write operation and can only be performed by a CAL.

Open any of the web views and see that each id is the body of one of the three files under `/ctl/cal`.

## Slaves

It it recommend that you add at least one slave to this cluster of three.  To
accomplish this, add another slave as shown above, but omit the step of
creating a new empty file under '/ctl/cal'.
