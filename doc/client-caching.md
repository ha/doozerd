# Consistent Cacheing

When you make a read request, the server response will contain a "cacheable"
flag. If this flag is set, you should cache the response. If this flag is
clear, you must not cache the response.

When you make a checkin request, the server response will contain a list of
invalidations. You must remove your cache entry for each element in the list.
You must also include the list of entries you've invalidated in your next
checkin message.
