package reflex

// EventFilter takes an Event and returns true if it should be allowed to be processed or
// false if it shouldn't. It can error if it fails to determine if the event should be processed.
// Please note it is expected that the func should return promptly and as such other than smaller
// in memory transforms/extractions it should not be making any I/O or significant API calls
// (especially remote ones) as the expectation is that the only data needed will be on the event
// itself.
type EventFilter func(event *Event) (bool, error)
