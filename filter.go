package reflex

import "errors"

// EventFilter takes a Event and returns true if it should be allowed to be processed or
// false if it shouldn't. It can error if it fails to determine if the event should be processed.
type EventFilter func(event *Event) (bool, error)

// AllEventFilters combines all its supplied EventFilter parameters and generates a single EventFilter
// that return true if all the EventFilter parameters return true and errors if any of them errors.
func AllEventFilters(efs ...EventFilter) EventFilter {
	return func(event *Event) (bool, error) {
		for _, ef := range efs {
			ok, err := ef(event)
			if !ok || err != nil {
				return false, err
			}
		}
		return true, nil
	}
}

// AnyEventFilters combines all its supplied EventFilter parameters and generates a single EventFilter
// that return true if any of the EventFilter parameters return true and otherwise return false together
// with any combination of errors returned from each of the filters.
func AnyEventFilters(efs ...EventFilter) EventFilter {
	return func(event *Event) (bool, error) {
		var allErr []error = nil
		for _, ef := range efs {
			ok, err := ef(event)
			if err != nil {
				allErr = append(allErr, err)
			} else if ok {
				return true, nil
			}
		}
		return false, errors.Join(allErr...)
	}
}
