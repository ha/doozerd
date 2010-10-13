# Consistent Cacheing

*These rules are derived from [Chubby][], which, in turn, derived its caching
model from distributed file systems such as AFS and Echo.*

 * Server's reply to a client read request must indicate whether the response
   is cacheable or not.

 * We should use an invalidation-only/update-never strategy. The server never
   sends updates to clients; it sends only invalidations.

 * Receiving a cacheable response implicitly subscribes the client to get a
   single invalidation notice from the server.

 * Whenever a write operation is being carried out, the server sends an
   invalidation to all subscribed clients.

 * When a client receives an invalidation notice, it acknowledges receipt in
   its next checkin message.

 * Server responses to write operations block until all invalidations have
   been sent and acknowledged.

 * Read requests are served during a pending write operation, but responses
   are marked as not cacheable.

[Chubby]: http://labs.google.com/papers/chubby.html
