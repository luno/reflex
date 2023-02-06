// Package rblob leverages the gocloud.dev/blob package and provides
// a reflex stream for events persisted in a bucket of strictly ordered
// append-only log of flat files.
//
// rblob provides at-least-once delivery semantics ONLY in the following conditions:
//   - Blobs (files) in the bucket must a strictly ordered append-only log. A new blob
//     must be become available for reading as the last blob in the log. This ensures
//     blobs are never skipped.
//   - Blobs must be immutable, they may not be modified or deleted prematurely. This
//     ensures a consistent ordered log.
//
// This is most commonly achieved by a single writer that a) writes blobs ordered (named)
// by timestamp and b) writes blobs slow enough that they become available for
// reading in order they are written. The resulting bucket of an AWS Kineses
// Firehouse is a perfect example.
package rblob
