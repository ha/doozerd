# Doozer URIs

This document describes the `doozer:` URI scheme.

A doozer link identifies a doozer cluster and provides
hints on how to contact the cluster. It contains a
sequence of parameters, the order of which is not
significant, formatted in the same way as the HTTP URL
query string.

There are three parameters:

 * *un* ("unique name"): a Base32-encoded 160-bit value
   unique to the doozer cluster that created it.

   Example:

       un=BCIRJYENEZYYYA5K65TY3C2SSZLGKW2K

   [TODO specify address discovery mechanisms, such as
   looking up addresses in another doozer cluster]

 * *cn* ("cluster name"): an ASCII value representing the name of the cluster
   given to `doozerd` with the `-c` flag.

   Example:

       cn=example

 * *ca* ("cluster address"): a host name or ip address,
   with an optional port suffix.

   [TODO reword this for clarity and specificity]

   1. IP address with port: connect directly.
   2. IP address without port: port is 46.
   3. Host name with port: look up IP address in DNS.
   4. Host name without port:
      1. look up SRV record in DNS; if found, use that.
      2. otherwise, look up IP address in DNS, and use
         port 46.

   This parameter can appear more than once, to provide
   more than one address through which to access the
   cluster.

   Examples:

       ca=10.0.0.1:5003
       ca=d.example.net

  * *sk* ("secret key"): an arbitrary string of characters clients must send to
    the server (via the `ACCESS` verb) before reading or writing.

  Example:

      sk=eXampl3

Full Example:

    doozer:?un=BCIRJYENEZYYYA5K65TY3C2SSZLGKW2K&ca=10.0.1.1&ca=10.0.1.2&ca=10.0.1.3
