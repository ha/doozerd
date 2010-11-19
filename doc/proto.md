# Client Protocol

Every request is formatted in three parts: verb, opid, data.

Every response is formatted in three parts: opid, flags, data.

    The protocol as of: Mon Nov  8 20:32:21 PST 2010

    VERB    DATA                 RETURN DATA
    ----    ----                 -----------------------

    # Close the response opid so that it will never be used again.
    CLOSE   opid                 +OK

    # Save a point-in-time snapshot for future reference.
    SNAP                         sid

    # I'll give you 1 guess.
    SET     [path body cas]      cas

    # Get a value from snap `sid`.  If `sid` is `0` then value
    # is taken from current live tree.
    GET     [path sid]           body

    DEL     path                 +OK

    # Set a paths value to the servers Nanoseconds()+interval
    SETT    [interval cas]       cas

    # Increment the servers seqn without mutation.
    NOOP    nil                  +OK

    # Walk tree `sid` SAX style.
    WALK    [glob sid]           [path body cas] ...

    # Watch tree.
    WATCH   glob                 [path body cas] ...

## Response Flags:

    Closed = 1 << iota
      The opid in given in the response is closed.

      The trailing arguments are to be ignored. This indicates to clients that
      they should clean-up any listeners for that opid.

    Last
      Same as above except the trailing arguments are valid.
