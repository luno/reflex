package reflex

import (
	"time"
)

type StreamOptions struct {
	// lag defines the duration after an event is created before it becomes
	// eligible for streaming.
	Lag time.Duration

	// StreamFromHead defines that the initial event be retrieved
	// from the head of the evens table.
	StreamFromHead bool
}

type StreamOption func(*StreamOptions)

// WithStreamFromHead provides an option to stream only new events from
// from the head of events table. Note this overrides the lastID parameter.
func WithStreamFromHead() StreamOption {
	return func(sc *StreamOptions) {
		sc.StreamFromHead = true
	}
}

// WithStreamLag provides an option to stream events only after they are older than a duration.
func WithStreamLag(d time.Duration) StreamOption {
	return func(sc *StreamOptions) {
		sc.Lag = d
	}
}
