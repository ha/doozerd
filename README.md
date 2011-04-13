# Doozer

## What Is It?

Doozer is a highly-available, completely consistent
store for small amounts of extremely important data.
When the data changes, it can notify connected clients
immediately (no polling), making it ideal for
infrequently-updated data for which clients want
real-time updates. Doozer is good for name service,
database master elections, and configuration data shared
between several machines. See *When Should I Use It?*,
below, for details.

See the [mailing list][mail] to discuss doozer with
other users and developers.

## Quick Start

1. Download [doozerd](https://github.com/ha/doozer/downloads)
2. Unpack the archive
3. Start a doozerd

        $ doozerd

4. Set a key and read it back

        $ echo "hello, world" | doozer set /message 0
        $ doozer get /message
        11046 13
        hello, world

5. Open <http://localhost:8080> and see your message

![doozer web view](/ha/doozer/raw/0a3a9c/doc/webview.png)

## How Does It Work?

Doozer is a network service. A handful of machines
(usually three, five, or seven) each run one doozer
server process. These processes communicate with each
other using a standard fully-consistent distributed
consensus algorithm. Clients dial in to one or more of
the doozer servers, issue commands, such as GET, SET,
and WATCH, and receive responses.

(insert network diagram here)

Each doozerd process has a complete copy of the
datastore and serves both read and write requests; there
is no distinguished "master" or "leader". Doozer is
designed to store data that fits entirely in memory; it
never writes data to permanent files. A separate tool
provides durable storage for backup and recovery.

## When Should I Use It?

Here are some example scenarios:

1. *Name Service*

    You have a set of machines that serve incoming HTTP
    requests. Due to hardware failure, occasionally one
    of these machines will fail and you replace it with a
    new machine at a new network address. A change to DNS
    data would take time to reach all clients, because
    the TTL of the old DNS record would cause it to
    remain in client caches for some time.

    Instead of DNS, you could use Doozer. Clients can
    subscribe to the names they are interested in, and
    they will get notified when any of those names&#8217;
    addresses change.

2. *Database Master Election*

    You are deploying a MySQL system. You want it to have
    high availability, so you add slaves on separate
    physical machines. When the master fails, you might
    promote one slave to become the new master. At any
    given time, clients need to know which machine is the
    master, and the slaves must coordinate with each
    other during failover.

    You can use doozer to store the address of the
    current master and all information necessary to
    coordinate failover.

3. *Configuration*

    You have processes on several different machines, and
    you want them all to use the same config file, which
    you must occasionally update. It is important that
    they all use the same configuration.

    Store the config file in doozer, and have the
    processes read their configuration directly from
    doozer.

## What can I do with it?

For a list of commands see the [protocol spec](https://github.com/ha/doozer/blob/master/doc/proto.md)

## Similar Projects

Doozer is similar to the following pieces of software:

 * Apache Zookeeper <http://zookeeper.apache.org/>
 * Google Chubby <http://labs.google.com/papers/chubby.html>

## Hacking on Doozer

 * [hacking on doozer](/ha/doozer/blob/master/doc/hacking.md)
 * [mailing list][mail]

## License and Authors

Doozer is distributed under the terms of the MIT
License. See [LICENSE][] for details.

Doozer was created by Blake Mizerany and Keith Rarick.
Type `git shortlog -s` for a full list of contributors.

[mail]: https://groups.google.com/group/doozer
[LICENSE]: /ha/doozer/blob/master/LICENSE
