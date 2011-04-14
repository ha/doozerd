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
respectively. See `src/pkg/proto/msg.proto` for their
definitions.

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

 * *set* = 4

    This response represents a mutation event that set a key.

 * *del* = 8

    This response represents a mutation event that deleted a key.

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
and writes, which are atomic. The store also records
the *revision* of each write.
This number can be given to a subsequent write
operation to ensure that no
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

 * `DEL` *path*, *rev* &rArr; &empty;

    Del deletes the file at *path* if *rev* is greater than
    or equal to the file's revision.

 * `GET` *path*, *rev* &rArr; *value*, *rev*

    Gets the contents (*value*) and revision (*rev*)
    of the file at *path* in the specified revision (*rev*).
    If *rev* is not provided, get uses the current revision.

 * `GETDIR` *path*, *rev*, *offset*, *limit* &rArr; {*path*}+

    Returns a sequence of responses containing the names
    of entries in *path* (a directory) in the specified
    revision (*rev*), in lexical order. It is an error
    if *path* is not a directory.

    If *offset* is given, getdir skips that many entries
    before returning any.

    If *limit* is given, getdir will send that many
    responses, at most.

 * `JOIN` (deprecated)

 * `NOP` (deprecated)

 * `REV` &empty; &rArr; *rev*

    Returns the current revision.

 * `SET` *path*, *rev*, *value* &rArr; *rev*

    Sets the contents of the file at *path* to *value*,
    as long as *rev* is greater than or equal to the file's
    revision.
    Returns the file's new revision.

 * `WALK` *path*, *rev* &rArr; {*path*, *rev*, *value*}+

    Iterates over all existing files that match *path*, a
    glob pattern, in revision *rev*. Sends one response
    for each matching file. If *rev* is not provided, walk
    uses the current revision.

    Glob notation:
     - `?` matches a single char in a single path component
     - `*` matches zero or more chars in a single path component
     - `**` matches zero or more chars in zero or more components
     - any other sequence matches itself

 * `WATCH` *path* &rArr; {*path*, *rev*, *value*}+

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

 * `REV_MISMATCH`

    A write operation has failed because the revision given
    was less than the revision of the file being set.

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

<style>
body { margin: 2em 10em }
p { max-width: 30em }
</style>

[protobuf]: http://code.google.com/p/protobuf/
[9P]: http://plan9.bell-labs.com/magic/man2html/5/intro
[data]: data-model.md
