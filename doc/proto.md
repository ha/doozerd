# Client Protocol

## Overview

[TODO: what is doozer]

(Note: this protocol is partially based on [9P][],
the Plan 9 file protocol. Parts of this document
are paraphrased from the 9P man pages.)

The doozer protocol is used for messages between clients
and servers. A client connects to doozerd by TCP and
transmits *request* messages to a server, which
subsequently returns *response* messages to the client.

Each message consists of a sequence of bytes comprising
two parts. First, a four-byte header field holds an
unsigned integer, *n*, in big-endian order (most
significant byte first). This is followed by *n* bytes
of data; these *n* bytes represent structured data
encoded in [Protocol Buffer][protobuf] format.

Two Protocol Buffer structures, `Request` and
`Response`, are used for *requests* and *responses*,
respectively. These structures are defined as follows
(in Protocol Buffers syntax):

    message Request {
      required int32 tag = 1;
      required Verb verb = 2;

      optional int64 cas = 3;
      optional string path = 4;
      optional bytes value = 5;
      optional int32 id = 6;
      optional int32 offset = 7;
      optional int32 limit = 8;
    }

    message Response {
      required int32 tag = 1;
      required int32 flags = 2;

      optional int64 seqn = 3;
      optional int64 cas = 4;
      optional string path = 5;
      optional bytes value = 6;
      optional int32 id = 7;

      optional Err err_code = 100;
      optional string err_detail = 101;
    }

Each request contains at least a tag, described below,
and a verb, to identify what action is desired.
The other fields may or may not be required; their
meanings depend on the verb, as described below.

The tag is chosen and used by the client to identify
the message. The reply (or replies) to the message
will have the same tag. Clients must arrange that no
two outstanding requests on the same connection have
the same tag.

Each response contains at least a tag and a flags field.
Other response fields may or may not be present,
depending on the verb of the request.

The flags field is a bitwise combination of the
following values:

 * *Valid* = 1

   If this flag is set, the response contains valid data
   and should be delivered to the user.

   Usually set, but can be unset, for example, in
   response to `WALK`, after the last file entry has
   been sent to the client.

 * *Done* = 2

   This is the last response for the given tag.
   The client is free to reuse the tag in another
   request.

A client can send multiple requests without waiting for
the corresponding responses, but all outstanding
requests must specify different tags. The server may
delay the response to a request and respond to later
ones; this is sometimes necessary, for example when the
client has issued a `WATCH` request and the responses
are sent after files are modified in the future.

### Ephemeral Files

[not yet implemented]

## Verbs

 * `CANCEL` = 10

   A request can be aborted with a cancel request. When
   a server receives a cancel, it will not reply to the
   message with tag *id*, and it will immediately reply.
   The client must wait until it gets the reply (even if
   the reply to the original message arrives in the
   interim), at which point tag *id* may be reused.

   The reply will set no fields.

 * `CHECKIN` = 0

   Used to establish and maintain a session, required if
   the client wishes to create ephemeral files or obtain
   ephemeral locks.

   Uses *cas* and *path*. Writes a file named *path* in
   directory `/session`. The contents of this file will be
   a decimal number of nanoseconds since January 1,
   1970\. This time is the session's *deadline*. It is
   determined by the server; and is typically several
   seconds after the checkin request message was
   received.

   If *cas* is 0, the file will be created only if it did
   not exist. Otherwise, the request *cas* is customarily
   -1, which means the file should be written
   unconditionally.

   If the current time passes the session's deadline,
   the file will be deleted by the doozer cluster. Any
   process can watch for the file to be deleted; this
   indicates the session has expired.

   Thus, successive checkin requests change the
   session's deadline further in the future each time,
   preventing it from expiring.

   Finally, the server will postpone its response to the
   client until shortly before the deadline. The client
   need not create any timers, inspect the value of this
   file, or otherwise obtain the session's deadline. It
   suffices to wait for the response to the checkin
   request, then immediately issue another checkin
   request.

   A successful response contains only the *cas* field,
   and its value is always -1.

 * `DEL` = 3
 // cas, path        => {}

 * `DELSNAP` = 6
 // id               => {}

 * `ELOCK`
 // future

 * `ESET`     = 4  // cas, path        => {}
 // future

 * `GET` = 1
 // path, id         => cas, value

 * `GETDIR`   = 14 // path             => {cas, value}+
 // future

 * `LOCK`
 // future

 * `JOIN`     = 13
 // deprecated

 * `MONITOR`  = 11 // path             => {cas, path, value}+
 // future

 * `NOOP` = 7
 // {}               => {}

 * `SET` = 2
 // cas, path, value => cas

 * `SNAP` = 5
 // {}               => seqn, id

 * `SYNCPATH` = 12 // path             => cas, value
 // future

 * `WATCH` = 8
 // path             => {cas, path, value}+

 * `WALK` = 9
 // path, id         => {cas, path, value}+

## Errors

The server might send a response with the `err_code` field
set. In that case, `err_detail` might also be set, and
the other optional response fields will be unset.

If `err_detail` is set, it provides extra information as
defined below.

Error codes are defined with the following meanings:

 * `TAG_IN_USE` = 1

   The server has noticed that the client sent two
   or more requests with the same tag. This is a
   serious error and always indicates a bug in the
   client.

   The server is not guaranteed to send this error.

 * `UNKNOWN_VERB` = 2

   The verb used in the request is not in the list of
   verbs defined in the server.

 * `REDIRECT` = 3

   Deprecated. Subject to change.

 * `INVALID_SNAP` = 4

   The snapshot id given in the request is invalid;
   there is no snapshot with that id.

 * `CAS_MISMATCH` = 5

   A set request has failed because the CAS token given
   did not match the CAS token of the file being set.

 * `BAD_PATH` = 6

   The given path contains invalid characters.

 * `MISSING_ARG` = 7

   The request's verb requires certain fields to be set
   and at least one of those fields was not set.

 * `NOTDIR` = 20

   The request operates only on a directory, but the
   given path is not a directory (either because it is a
   file or it is missing).

 * `ISDIR` = 21

   The request operates only on a regular file, but the
   given path is a directory.

 * `OTHER` = 127

   Some other error has occurred. The `err_detail`
   string provides a description.

Error value 0 is reserved.

## Examples

(In these examples, we'll use an informal notation
similar to JSON to indicate the contents of structures
sent over the wire.)

### Get

Let's say the client wants to retrieve file /a. So it
sends the following:

    {
        tag:  0,
        verb: GET,
        path: "/a",
    }

The server replies

    {
        tag:   0,
        flags: 3, // 3 == Valid|Done
        cas:   5,
        value: "hello",
    }

### Set and Get

Set usually takes much longer than get, so here we'll
see replies come out of order:

    {
        tag:   0,
        verb:  SET,
        path:  "/a",
        cas:   -1,
        value: "goodbye",
    }

    {
        tag:  1,
        verb: GET,
        path: "/a",
    }

The server replies immediately:

    {
        tag:   1,
        flags: 3, // 3 == Valid|Done
        cas:   5,
        value: "hello",
    }

Some time later, the set operation finishes:

    {
        tag:   0,
        flags: 3, // 3 == Valid|Done
        cas:   6,
    }

Now, the client can issue the same get request once
more:

    {
        tag:  1,
        verb: GET,
        path: "/a",
    }

This time, the server replies:

    {
        tag:   1,
        flags: 3, // 3 == Valid|Done
        cas:   6,
        value: "goodbye",
    }

<style>
p { max-width: 30em }
</style>

[protobuf]: http://code.google.com/p/protobuf/
[9P]: http://plan9.bell-labs.com/magic/man2html/5/intro
