// Package recordcodec owns the versioned JSON envelope used by durable records.
package recordcodec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
)

type recordEnvelope[T any] struct {
	Schema int    `json:"schema"`
	Kind   string `json:"kind"`
	Data   T      `json:"data"`
}

func Encode[T any](kind string, record T) ([]byte, error) {
	value, err := json.Marshal(recordEnvelope[T]{Schema: 1, Kind: kind, Data: record})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return value, nil
}

func Decode[T any](value []byte, kind string) (T, error) {
	var zero T
	if err := RejectDuplicateFields(value); err != nil {
		return zero, errs.New(errs.KindInternal, "durable record contains duplicate or malformed JSON fields")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var envelope recordEnvelope[T]
	if err := decoder.Decode(&envelope); err != nil {
		return zero, errs.New(errs.KindInternal, "durable record schema is invalid")
	}
	if err := RequireEOF(decoder); err != nil {
		return zero, errs.New(errs.KindInternal, "durable record has trailing JSON data")
	}
	if envelope.Schema != 1 || envelope.Kind != kind {
		return zero, errs.New(errs.KindInternal, "durable record envelope does not match its repository")
	}
	return envelope.Data, nil
}

func RejectDuplicateFields(value []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	if err := walkJSONValue(decoder); err != nil {
		return err
	}
	return RequireEOF(decoder)
}

func walkJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("JSON object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate JSON field %q", key)
			}
			seen[key] = struct{}{}
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return fmt.Errorf("JSON object is not closed")
		}
	case '[':
		for decoder.More() {
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return fmt.Errorf("JSON array is not closed")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	return nil
}

func RequireEOF(decoder *json.Decoder) error {
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}
