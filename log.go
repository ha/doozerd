// Package log implements a persistence layer for doozer using a circular log.
// Writes containing mutations are issued at the end of the log.  Deletions
// are garbage collected from the head of the logso that the backing file
// does not grow indefinetly.

package log
