package api

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func rejectDuplicateJSONMembers(value []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	if err := scanUniqueJSONValue(decoder); err != nil {
		return errs.New(errs.KindMalformedRequest, "backup policy request contains invalid or duplicate members")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errs.New(errs.KindMalformedRequest, "backup policy request must contain one JSON document")
	}
	return nil
}

func scanUniqueJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	if delimiter == '[' {
		for decoder.More() {
			if err := scanUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	}
	if delimiter != '{' {
		return io.ErrUnexpectedEOF
	}
	seen := map[string]struct{}{}
	for decoder.More() {
		member, err := decoder.Token()
		if err != nil {
			return err
		}
		name, ok := member.(string)
		if !ok {
			return io.ErrUnexpectedEOF
		}
		if _, duplicate := seen[name]; duplicate {
			return errs.New(errs.KindMalformedRequest, "duplicate JSON member")
		}
		seen[name] = struct{}{}
		if err := scanUniqueJSONValue(decoder); err != nil {
			return err
		}
	}
	_, err = decoder.Token()
	return err
}
