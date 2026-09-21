package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
	"unicode/utf8"
)

func decodeEntryJSONObject(body []byte, label string) (map[string]json.RawMessage, error) {
	if !utf8.Valid(body) {
		return nil, errs.New(errs.KindMalformedRequest, label+" body is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil {
		return nil, errs.Wrap(errs.KindMalformedRequest, err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return nil, errs.New(errs.KindMalformedRequest, label+" body must be an object")
	}
	members := make(map[string]json.RawMessage)
	for decoder.More() {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			clearEntryJSONMembers(members)
			return nil, errs.Wrap(errs.KindMalformedRequest, tokenErr)
		}
		name, ok := token.(string)
		if !ok {
			clearEntryJSONMembers(members)
			return nil, errs.New(errs.KindMalformedRequest, label+" member name is invalid")
		}
		if _, duplicate := members[name]; duplicate {
			clearEntryJSONMembers(members)
			return nil, errs.New(errs.KindMalformedRequest, label+" body contains a duplicate member")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			clearEntryJSONMembers(members)
			return nil, errs.Wrap(errs.KindMalformedRequest, err)
		}
		members[name] = value
	}
	closing, err := decoder.Token()
	if err != nil {
		clearEntryJSONMembers(members)
		return nil, errs.Wrap(errs.KindMalformedRequest, err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		clearEntryJSONMembers(members)
		return nil, errs.New(errs.KindMalformedRequest, label+" body is malformed")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		clearEntryJSONMembers(members)
		if err == nil {
			err = errors.New("trailing JSON value")
		}
		return nil, errs.Wrap(errs.KindMalformedRequest, err)
	}
	return members, nil
}

func decodeEntryJSONMember[T any](raw json.RawMessage, target *T) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errs.New(errs.KindValidationFailed, "entry request member must not be null")
	}
	if err := json.Unmarshal(raw, target); err != nil {
		var typeError *json.UnmarshalTypeError
		if errors.As(err, &typeError) {
			return errs.New(errs.KindValidationFailed, "entry request member has an invalid type")
		}
		return errs.Wrap(errs.KindMalformedRequest, err)
	}
	return nil
}

func clearEntryJSONMembers(members map[string]json.RawMessage) {
	for name, value := range members {
		clear(value)
		delete(members, name)
	}
}
