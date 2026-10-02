// Package dotenvfile owns the bounded Compose dotenv encoding used by ordinary
// configuration and restored captured values. Selection belongs to the caller.
package dotenvfile

import (
	"bytes"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/compose-spec/compose-go/v2/dotenv"
)

type Value struct {
	Name    string
	Content []byte
}

// Render owns its output, not input bytes. It rejects a scope exceeding the
// existing helper ceiling before allocating an oversized generated file.
func Render(values []Value) ([]byte, error) {
	ordered := append([]Value(nil), values...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	var size uint64
	for index, value := range ordered {
		if value.Name == "" || bytes.IndexByte([]byte(value.Name), '=') >= 0 ||
			bytes.ContainsAny([]byte(value.Name), "\x00\r\n") || bytes.IndexByte(value.Content, 0) >= 0 {
			return nil, errs.New(
				errs.KindValidationFailed,
				"generated environment contains an invalid name or NUL value",
			)
		}
		if index > 0 && ordered[index-1].Name == value.Name {
			return nil, errs.New(errs.KindValidationFailed, "duplicate env key in one exposure scope")
		}
		size += uint64(len(value.Name)) + 4
		for _, character := range value.Content {
			size++
			if escaped(character) {
				size++
			}
		}
		if size > entrymaterialization.MaximumContentBytes {
			return nil, errs.New(
				errs.KindValidationFailed,
				"generated environment exceeds the materialization byte limit",
			)
		}
	}
	output := make([]byte, 0, int(size))
	for _, value := range ordered {
		output = append(output, value.Name...)
		output = append(output, '=', '"')
		for _, character := range value.Content {
			switch character {
			case '\\', '"':
				output = append(output, '\\', character)
			case '$':
				output = append(output, '$', '$')
			case '\n':
				output = append(output, '\\', 'n')
			case '\r':
				output = append(output, '\\', 'r')
			case '\t':
				output = append(output, '\\', 't')
			case '\a':
				output = append(output, '\\', 'a')
			case '\b':
				output = append(output, '\\', 'b')
			case '\f':
				output = append(output, '\\', 'f')
			case '\v':
				output = append(output, '\\', 'v')
			default:
				output = append(output, character)
			}
		}
		output = append(output, '"', '\n')
	}
	parsed, err := dotenv.ParseWithLookup(bytes.NewReader(output), func(string) (string, bool) { return "", false })
	if err != nil || len(parsed) != len(ordered) {
		clear(output)
		return nil, errs.New(errs.KindInternal, "generated environment changed membership when parsed")
	}
	for _, value := range ordered {
		actual, found := parsed[value.Name]
		if !found || !bytes.Equal([]byte(actual), value.Content) {
			clear(output)
			return nil, errs.New(errs.KindInternal, "generated environment changed a value when parsed")
		}
	}
	return output, nil
}

func escaped(character byte) bool {
	switch character {
	case '\\', '"', '$', '\n', '\r', '\t', '\a', '\b', '\f', '\v':
		return true
	default:
		return false
	}
}
