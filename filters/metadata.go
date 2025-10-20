package filters

import (
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"

	"github.com/luno/reflex"
)

type (
	Deserializer[T any] func(b []byte) (T, error)
	DataFilter[T any]   func(d T) (bool, error)
)

const (
	deserializationErrMsg     = "deserialization failed"
	metadataEventFilterErrMsg = "cannot make a MetadataEventFilter from a nil Deserializer or DataFilter"
)

var (
	metadataEventFilterErr = errors.New(metadataEventFilterErrMsg, j.C("ERR_c1f2e3d4a5b6c7d8"))
	deserializationErr     = errors.New(deserializationErrMsg, j.C("ERR_a1b2c3d4e5f6a7b8"))
)

func MetadataEventFilter[T any](ds Deserializer[T], flt DataFilter[T]) (reflex.EventFilter, error) {
	if ds == nil || flt == nil {
		return nil, makeMetadataEventFilterErr(j.MKV{"ds": ds, "flt": flt})
	}
	return func(e *reflex.Event) (bool, error) {
		var b []byte
		if e != nil {
			b = e.MetaData
		}
		d, err := ds(b)
		if err != nil {
			return false, asDeserializationErr(err)
		}
		return flt(d)
	}, nil
}

// IsDeserializationErr returns true if the error occurred during Metadata deserialization operations.
func IsDeserializationErr(err error) bool {
	return errors.Is(err, deserializationErr)
}

func asDeserializationErr(err error) error {
	return errors.Wrap(err, deserializationErrMsg, j.C("ERR_a1b2c3d4e5f6a7b8"))
}

// IsMetadataEventFilterErr returns true if the error occurred during construction of a MetadataEventFilter.
func IsMetadataEventFilterErr(err error) bool {
	return errors.Is(err, metadataEventFilterErr)
}

func makeMetadataEventFilterErr(ol ...errors.Option) error {
	opts := append([]errors.Option{j.C("ERR_c1f2e3d4a5b6c7d8")}, ol...)
	return errors.New(metadataEventFilterErrMsg, opts...)
}
