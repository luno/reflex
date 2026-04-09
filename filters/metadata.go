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

	metadataEventFilterErrCode = "ERR_1a5f8c3e7b2d4f09"
	deserializationErrCode     = "ERR_7e3f5b8a1c4d2e96"
)

var (
	metadataEventFilterErr = errors.New(metadataEventFilterErrMsg, j.C(metadataEventFilterErrCode))
	deserializationErr     = errors.New(deserializationErrMsg, j.C(deserializationErrCode))
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
	return errors.Wrap(err, deserializationErrMsg, j.C(deserializationErrCode))
}

// IsMetadataEventFilterErr returns true if the error occurred during construction of a MetadataEventFilter.
func IsMetadataEventFilterErr(err error) bool {
	return errors.Is(err, metadataEventFilterErr)
}

func makeMetadataEventFilterErr(ol ...errors.Option) error {
	ol = append(ol, j.C(metadataEventFilterErrCode))
	return errors.New(metadataEventFilterErrMsg, ol...)
}
