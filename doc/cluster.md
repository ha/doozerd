# Clustering Doozer

## Terms

**Cluster**
	
One or more members, connected over a network, that achieve total ordering of all writes to the data store through consensus.

**Member**

A `doozerd` participating in consenses with itself and/or others over a network.

**Seed**

The initial member of a cluster.

**Slave**

A `doozerd` attached to a cluster that learns values so it may take overfor a dead member if the time comes.

**DzNS**

Stands for *Doozer Name Service* which is a cluster used for bootstrapping and discovering doozer clusters.

## How to create a cluster of three members and one slave

### Without DzNS

Start the seed:

		$ doozerd -l 127.0.0.1:8046

Attach three slaves (this can be done in parellel):

		$ doozerd -l 127.0.0.2:8046 -a 127.0.0.1:8046
		$ doozerd -l 127.0.0.3:8046 -a 127.0.0.1:8046
		$ doozerd -l 127.0.0.4:8046 -a 127.0.0.1:8046

NOTE:  It is not sufficiant to listen on IP '0.0.0.0'.  Doozer uses the address given as a form of identification.  You must use the address the other `doozerd` processes will connect with.

Create two extra member files so the slaves will attempt to become members:

		$ export DOOZER_URI='doozer:?ca=127.0.0.1:8046'
		$ printf '' | doozer set /ctl/cal/1 0
		$ printf '' | doozer set /ctl/cal/2 0

You can now open 127.0.0.X:8000 to see each member and the slaves web view and make sure each file `/ctl/cal` contains a valid identifier.

### With DzNS

First, create a cluster without DzNS.  This new cluster will be the DzNS the next cluster will use to bootstrap.

Now create another cluster of three with the DzNS (these can be done in parellel):

		$ export DOOZER_BOOT_URI='doozer:?ca=127.0.0.1:8046&ca=127.0.0.2:8046&ca=127.0.0.3:8046'
		$ doozerd -c example -l 127.0.0.11:8046
		$ doozerd -c example -l 127.0.0.12:8046
		$ doozerd -c example -l 127.0.0.13:8046
		$ doozerd -c example -l 127.0.0.14:8046

		$ export DOOZER_URI='doozer:?cn=example'
		$ printf '' | doozer set /ctl/cal/1 0
		$ printf '' | doozer set /ctl/cal/2 0
