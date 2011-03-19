# Client Protocol

## Overview

Doozer is a highly-available, consistent lock service.
It also lets you store small amounts of metadata as
files in a directory tree. See below for a complete
description of the data model.

The doozer protocol is used for messages between clients
and servers. A client connects to doozerd by TCP and
transmits *request* messages to a server, which
subsequently returns *response* messages to the client.

(Note: this protocol is partially based on [9P][],
the Plan 9 file protocol. Parts of this document
are paraphrased from the 9P man pages.)

Each message consists of a sequence of bytes comprising
two parts. First, a four-byte header field holds an
unsigned integer, *n*, in big-endian order (most
significant byte first). This is followed by *n* bytes
of data; these *n* bytes represent structured data
encoded in [Protocol Buffer][protobuf] format.

Two Protocol Buffer structures, `Request` and
`Response`, are used for *requests* and *responses*,
respectively. These structures are defined as follows
(in Protocol Buffer syntax):

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

      optional int64 rev = 3;
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

Some requests will cause the server to send
more than one response, depending on the verb.
A request is considered outstanding until
the last response has been received by the client.
The last response will be marked with the *done* flag,
described below.

The flags field is a bitwise combination of the
following values:

 * *valid* = 1

   If this flag is set, the response contains valid data.
   If unset, the client should ignore all fields except
   *tag* and *flags*.

 * *done* = 2

   This is the last response for the given *tag*.
   After a response with this flag has been received,
   the client is free to reuse its tag in another
   request (unless there is a pending cancel
   transaction for that tag; see `CANCEL` below).

A client can send multiple requests without waiting for
the corresponding responses, but all outstanding
requests must specify different tags. The server may
delay the response to a request and respond to later
ones; this is sometimes necessary, for example when the
client has issued a `WATCH` request and the responses
are sent after files are modified in the future.

### Data Model

For a thorough description of Doozer's data model,
see [Data Model][data]. Briefly, doozer's store holds
a tree structure of files identified by paths similar
to paths in Unix, and performs only whole-file reads
and writes, which are atomic. The store also records a
*Compare-and-Set* (CAS) token with each write.
This token can be given to a subsequent write
operation on the same file to assure that no
intervening writes have happened.

## Verbs

Each verb shows the set of request fields it uses,
followed by the set of response fields it provides.
Some requests can result in more than one response.
This is indicated by a + sign after the response fields.

 * `CANCEL` *id* &rArr; &empty;

   A request can be aborted with a cancel request. When
   a server receives a cancel, it will not reply to the
   message with tag *id*, and it will immediately reply
   to the cancel request. The client must wait until it
   gets the reply (even if the reply to the original
   message arrives in the interim), at which point tag
   *id* may be reused.

 * `CHECKIN` *path*, *cas* &rArr; *cas*

   Used to establish and maintain a session, required if
   the client wishes to create ephemeral files or obtain
   ephemeral locks.

   Writes a file named *path* in directory `/ctl/session`.
   The contents of this file will be a decimal number of
   nanoseconds since January 1, 1970. This time is the
   session's *deadline*. It is determined by the server;
   and is typically several seconds after the checkin
   request message was received.

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

   The response *cas* field is always -1.

 * `DEL` *path*, *cas* &rArr; &empty;

   Del deletes the file at *path* if *cas* is greater than
   or equal to the file's CAS token.

 * `ELOCK` (not yet implemented)

 * `ESET` (not yet implemented)

 * `FLUSH` (not yet implemented)

   Flush causes the server to initiate consensus immediately
   with its current buffer of update proposals, rather than
   waiting for the next frame tick.

   Clients usually do not need to use this request.

 * `GET` *path*, *id* &rArr; *value*, *cas*

   Gets the contents (*value*) and CAS token (*cas*)
   of the file at *path*, in the *rev*.
   If *id* is 0 or unset, uses the current revision
   of the data store.

 * `GETDIR` (not yet implemented)

 * `LOCK` (not yet implemented)

   Obtains a lock on the file at *path*, waiting
   if necessary for the lock to be released by
   another client.

 * `JOIN` (deprecated)

 * `MONITOR` *path* &rArr; {*path*, *cas*, *value*}+

   Returns one response for each file matching *path*,
   a glob pattern, when the request is received, as well as
   one response (an *event*) for each subsequent change
   to a file matching *path*. Each event will have its *rev*
   field set, in addition to the fields listed above.
   No events will be sent until all existing files have been sent.

   Equivalent to `WALK` followed by `WATCH`, but guarantees that
   no two responses will have the same CAS token, and that
   the *rev* of the first event is one greater than the rev
   of the `WALK` rev.

 * `NOOP` (deprecated)

 * `REV` &empty; &rArr; *rev*

   Returns the current revision.

 * `SET` *path*, *cas*, *value* &rArr; *cas*

   Sets the contents of the file at *path* to *value*,
   as long as *cas* is greater than or equal to the file's
   old CAS token.
   Returns the new CAS token.

 * `SYNCPATH` (not yet implemented)

 * `WALK` *path*, *id* &rArr; {*path*, *cas*, *value*}+

   Iterates over all existing files that match *path*, a
   glob pattern, in *rev*. Sends one response
   for each matching file. If *id* is 0, uses the current
   state of the data store.

   Glob notation:
    - `?` matches a single char in a single path component
    - `*` matches zero or more chars in a single path component
    - `**` matches zero or more chars in zero or more components
    - any other sequence matches itself

 * `WATCH` *path* &rArr; {*rev*, *path*, *cas*, *value*}+

   Arranges for the client to receive notices of changes
   made to any file matching *path*, a glob pattern. One
   response will be sent for each change (either set or
   del). See above for glob notation.

## Errors

The server might send a response with the `err_code` field
set. In that case, `err_detail` might also be set, and
the other optional response fields will be unset.

If `err_detail` is set, it provides extra information as
defined below.

Error codes are defined with the following meanings:

 * `TAG_IN_USE`

   The server has noticed that the client sent two
   or more requests with the same tag. This is a
   serious error and always indicates a bug in the
   client.

   The server is not guaranteed to send this error.

 * `UNKNOWN_VERB`

   The verb used in the request is not in the list of
   verbs defined in the server.

 * `REDIRECT`

   Deprecated. Subject to change.

 * `TOO_LATE`

   The rev given in the request is invalid;
   it has been garbage collected.

   The current default of history kept is 360,000 revs.

 * `CAS_MISMATCH`

   A set request has failed because the CAS token given
   did not match the CAS token of the file being set.

 * `BAD_PATH`

   The given path contains invalid characters.

 * `MISSING_ARG`

   The request's verb requires certain fields to be set
   and at least one of those fields was not set.

 * `NOTDIR`

   The request operates only on a directory, but the
   given path is not a directory (either because it is a
   file or it is missing).

 * `ISDIR`

   The request operates only on a regular file, but the
   given path is a directory.

 * `OTHER`

   Some other error has occurred. The `err_detail`
   string provides a description.

Error value 0 is reserved.

## Examples

(In these examples, we'll use an informal notation
similar to JSON to indicate the contents of structures
sent over the wire.)

### Get

Let's say the client wants to retrieve file `/a`. So it
sends the following:

    {
        tag:  0,
        verb: GET,
        path: "/a",
    }

The server replies

    {
        tag:   0,
        flags: 3, // 3 == valid|done
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
        flags: 3, // 3 == valid|done
        cas:   5,
        value: "hello",
    }

Some time later, the set operation finishes:

    {
        tag:   0,
        flags: 3, // 3 == valid|done
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
        flags: 3, // 3 == valid|done
        cas:   6,
        value: "goodbye",
    }

<style>
p { max-width: 30em }
</style>

[protobuf]: http://code.google.com/p/protobuf/
[9P]: http://plan9.bell-labs.com/magic/man2html/5/intro
[data]: data-model.md
