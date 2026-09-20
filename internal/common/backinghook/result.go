package backinghook

import (
	"bytes"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ParseResult parses the private result file and returns every declared fact
// exactly once, in schema order. Values may be empty and retain every byte
// after the first equals sign on their line.
func ParseResult(schema []FactDefinition, content []byte) (Output, error) {
	var output Output
	if len(content) > MaximumResultBytes {
		return output, errs.New(errs.KindValidationFailed, "backing hook result exceeds its size limit")
	}
	if err := validateSchema(schema); err != nil {
		return output, err
	}
	values := make(map[string][]byte, len(schema))
	declared := make(map[string]FactDefinition, len(schema))
	for _, definition := range schema {
		declared[definition.Key] = definition
	}
	for _, line := range bytes.Split(content, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		separator := bytes.IndexByte(line, '=')
		if separator <= 0 {
			return output, errs.New(errs.KindValidationFailed, "backing hook result line is malformed")
		}
		key := string(line[:separator])
		if !ValidKey(key) {
			return output, errs.New(errs.KindValidationFailed, "backing hook result key is invalid")
		}
		if _, exists := declared[key]; !exists {
			return output, errs.New(errs.KindValidationFailed, "backing hook returned an undeclared fact")
		}
		if _, exists := values[key]; exists {
			return output, errs.New(errs.KindValidationFailed, "backing hook returned a duplicate fact")
		}
		values[key] = line[separator+1:]
	}
	for _, definition := range schema {
		if _, exists := values[definition.Key]; !exists {
			return output, errs.New(errs.KindValidationFailed, "backing hook omitted a declared fact")
		}
	}
	output.Facts = make([]Fact, 0, len(schema))
	for _, definition := range schema {
		output.Facts = append(output.Facts, Fact{
			Key: definition.Key, Value: append([]byte(nil), values[definition.Key]...), Secret: definition.Secret,
		})
	}
	if err := ValidateOutput(schema, output); err != nil {
		output.Clear()
		return Output{}, err
	}
	return output, nil
}

// ValidateOutput applies the same exact-schema rule to facts produced by
// built-in adapters and custom hook result files.
func ValidateOutput(schema []FactDefinition, output Output) error {
	if err := validateSchema(schema); err != nil {
		return err
	}
	if len(output.Facts) != len(schema) {
		return errs.New(errs.KindValidationFailed, "backing fact output is incomplete")
	}
	seen := make(map[string]struct{}, len(output.Facts))
	declared := make(map[string]FactDefinition, len(schema))
	encodedBytes := 0
	for _, definition := range schema {
		declared[definition.Key] = definition
	}
	for _, fact := range output.Facts {
		definition, exists := declared[fact.Key]
		if !exists || !ValidKey(fact.Key) || fact.Secret != definition.Secret ||
			bytes.IndexByte(fact.Value, 0) >= 0 || bytes.IndexByte(fact.Value, '\n') >= 0 {
			return errs.New(errs.KindValidationFailed, "backing fact output is invalid")
		}
		if _, duplicate := seen[fact.Key]; duplicate {
			return errs.New(errs.KindValidationFailed, "backing fact output is duplicated")
		}
		seen[fact.Key] = struct{}{}
		encodedBytes += len(fact.Key) + 2
		if encodedBytes > MaximumResultBytes || len(fact.Value) > MaximumResultBytes-encodedBytes {
			return errs.New(errs.KindValidationFailed, "backing fact output exceeds its size limit")
		}
		encodedBytes += len(fact.Value)
	}
	return nil
}
