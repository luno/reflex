package rblob

import (
	"encoding/json"

	"gocloud.dev/blob"
)

// JSONDecoder is the default decoder function that decodes blobs into
// raw json byte slices.
var JSONDecoder = func(r *blob.Reader) (Decoder, error) {
	return &jsonDecoder{
		decoder: json.NewDecoder(r),
	}, nil
}

type jsonDecoder struct {
	decoder *json.Decoder
}

func (d *jsonDecoder) Decode() ([]byte, error) {

	var raw json.RawMessage
	err := d.decoder.Decode(&raw)
	if err != nil {
		return nil, err
	}

	return raw, nil
}
