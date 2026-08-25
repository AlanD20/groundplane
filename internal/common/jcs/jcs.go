// Package jcs is the Groundplane boundary for RFC 8785 JSON.
//
// The upstream module is deliberately hidden behind this byte-only gate.
// Release callers cannot bypass the I-JSON checks or hand a mutable decode
// destination into this package.
package jcs

import (
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
	thirdparty "github.com/gowebpki/jcs"
)

var utf8BOM = []byte{0xef, 0xbb, 0xbf}

// Canonicalize validates one UTF-8 JSON object or array and returns its RFC
// 8785 bytes. It accepts ordinary valid JSON spellings and normalizes them;
// callers admitting already-sealed release bytes must use RequireCanonical.
func Canonicalize(raw []byte) ([]byte, error) {
	if err := validateInput(raw); err != nil {
		return nil, err
	}

	transformed, transformErr := thirdparty.Transform(raw)
	if transformErr != nil || len(transformed) == 0 {
		// The upstream implementation may return partial output with an error.
		// Never expose either value on that path.
		return nil, invalidJSON("json canonicalization failed")
	}
	if err := validateOutput(transformed); err != nil {
		return nil, err
	}
	return append([]byte(nil), transformed...), nil
}

// RequireCanonical admits only byte-exact RFC 8785 input. This rejects
// noncanonical numeric spellings, whitespace, escape spellings, and ordering.
func RequireCanonical(raw []byte) ([]byte, error) {
	canonical, err := Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(raw, canonical) {
		return nil, invalidJSON("json value is not canonical")
	}
	return canonical, nil
}

// Decode decodes canonical bytes into a fresh zero value and returns that
// value. It rejects unknown fields, trailing data, and any typed re-encoding
// that does not reproduce the exact canonical bytes. No caller-owned
// destination is mutated.
func Decode[T any](raw []byte) (T, error) {
	var zero T
	canonical, err := RequireCanonical(raw)
	if err != nil {
		return zero, err
	}

	var value T
	decoder := json.NewDecoder(bytes.NewReader(canonical))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return zero, invalidJSON("typed json decode failed")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return zero, invalidJSON("typed json decode has trailing data")
	}

	reencoded, err := json.Marshal(value)
	if err != nil {
		return zero, invalidJSON("typed json re-encode failed")
	}
	recanonical, err := Canonicalize(reencoded)
	if err != nil || !bytes.Equal(canonical, recanonical) {
		return zero, invalidJSON("typed json value does not round-trip")
	}
	return value, nil
}

func validateInput(raw []byte) error {
	if len(raw) == 0 || !utf8.Valid(raw) {
		return invalidJSON("json value is not valid utf-8")
	}
	if bytes.HasPrefix(raw, utf8BOM) {
		return invalidJSON("json value contains a utf-8 bom")
	}
	if !json.Valid(raw) {
		return invalidJSON("json value is not valid")
	}
	if !validRoot(raw) {
		return invalidJSON("json value root must be an object or array")
	}
	if !validSurrogateEscapes(raw) {
		return invalidJSON("json value contains an invalid utf-16 surrogate escape")
	}
	if !uniqueMembers(raw) {
		return invalidJSON("json value contains a duplicate member")
	}
	return nil
}

func validateOutput(raw []byte) error {
	if len(raw) == 0 || !utf8.Valid(raw) || bytes.HasPrefix(raw, utf8BOM) ||
		!json.Valid(raw) || !validRoot(raw) || !validSurrogateEscapes(raw) ||
		!uniqueMembers(raw) {
		return invalidJSON("json canonical output is invalid")
	}
	// Confirm the external implementation's output is itself stable. This is
	// an output check only; all errors and output from the first call were
	// already discarded on error.
	repeated, err := thirdparty.Transform(raw)
	if err != nil || !bytes.Equal(raw, repeated) {
		return invalidJSON("json canonical output is not stable")
	}
	return nil
}

func validRoot(raw []byte) bool {
	index := 0
	for index < len(raw) {
		switch raw[index] {
		case ' ', '\t', '\r', '\n':
			index++
		default:
			return raw[index] == '{' || raw[index] == '['
		}
	}
	return false
}

// validSurrogateEscapes applies RFC 8785's Unicode scalar-value rule to the
// raw JSON string escapes. A high surrogate must be immediately followed by a
// low surrogate; a low surrogate can never appear alone or first.
func validSurrogateEscapes(raw []byte) bool {
	inString := false
	for index := 0; index < len(raw); {
		if !inString {
			if raw[index] == '"' {
				inString = true
			}
			index++
			continue
		}

		switch raw[index] {
		case '"':
			inString = false
			index++
		case '\\':
			if index+1 >= len(raw) {
				return false
			}
			if raw[index+1] != 'u' {
				index += 2
				continue
			}
			if index+6 > len(raw) {
				return false
			}
			unit, ok := hexUint16(raw[index+2 : index+6])
			if !ok {
				return false
			}
			switch {
			case unit >= 0xd800 && unit <= 0xdbff:
				if index+12 > len(raw) || raw[index+6] != '\\' || raw[index+7] != 'u' {
					return false
				}
				low, ok := hexUint16(raw[index+8 : index+12])
				if !ok || low < 0xdc00 || low > 0xdfff {
					return false
				}
				index += 12
			case unit >= 0xdc00 && unit <= 0xdfff:
				return false
			default:
				index += 6
			}
		default:
			index++
		}
	}
	return !inString
}

func hexUint16(value []byte) (uint16, bool) {
	if len(value) != 4 {
		return 0, false
	}
	var result uint16
	for _, character := range value {
		var digit byte
		switch {
		case character >= '0' && character <= '9':
			digit = character - '0'
		case character >= 'a' && character <= 'f':
			digit = character - 'a' + 10
		case character >= 'A' && character <= 'F':
			digit = character - 'A' + 10
		default:
			return 0, false
		}
		result = result<<4 | uint16(digit)
	}
	return result, true
}

func uniqueMembers(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if !scanJSONValue(decoder) {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}

func scanJSONValue(decoder *json.Decoder) bool {
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return true
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			member, err := decoder.Token()
			if err != nil {
				return false
			}
			name, ok := member.(string)
			if !ok {
				return false
			}
			if _, duplicate := seen[name]; duplicate {
				return false
			}
			seen[name] = struct{}{}
			if !scanJSONValue(decoder) {
				return false
			}
		}
		closing, err := decoder.Token()
		return err == nil && closing == json.Delim('}')
	case '[':
		for decoder.More() {
			if !scanJSONValue(decoder) {
				return false
			}
		}
		closing, err := decoder.Token()
		return err == nil && closing == json.Delim(']')
	default:
		return false
	}
}

func invalidJSON(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}
