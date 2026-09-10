package api

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type scriptCreateFields ScriptCreate
type scriptEditFields ScriptEdit

type scriptCreateWire struct {
	scriptCreateFields
	Order     json.RawMessage `json:"order"`
	Execution json.RawMessage `json:"execution"`
}

type scriptEditWire struct {
	scriptEditFields
	Order     json.RawMessage `json:"order"`
	Execution json.RawMessage `json:"execution"`
}

// UnmarshalJSON distinguishes omitted default order from explicit JSON null.
func (input *ScriptCreate) UnmarshalJSON(value []byte) error {
	wire, err := decodeScriptRequest[scriptCreateWire](value)
	if err != nil {
		return err
	}
	order, err := decodeScriptOrder(wire.Order)
	if err != nil {
		return err
	}
	decoded := ScriptCreate(wire.scriptCreateFields)
	decoded.Execution, err = decodeScriptExecution(wire.Execution)
	if err != nil {
		return err
	}
	if order != nil {
		decoded.Order = *order
	}
	*input = decoded
	return nil
}

// UnmarshalJSON preserves an authored zero patch and refuses null-as-omission.
func (input *ScriptEdit) UnmarshalJSON(value []byte) error {
	wire, err := decodeScriptRequest[scriptEditWire](value)
	if err != nil {
		return err
	}
	order, err := decodeScriptOrder(wire.Order)
	if err != nil {
		return err
	}
	decoded := ScriptEdit(wire.scriptEditFields)
	decoded.Execution, err = decodeScriptExecution(wire.Execution)
	if err != nil {
		return err
	}
	decoded.Order = order
	*input = decoded
	return nil
}

func decodeScriptRequest[T scriptCreateWire | scriptEditWire | scriptExecutionWire | scriptVolumeGrantWire](
	value []byte,
) (T, error) {
	var wire T
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return wire, errs.New(errs.KindMalformedRequest, "Script request must be an object")
	}
	if err := rejectDuplicateJSONMembers(value); err != nil {
		return wire, err
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return wire, errs.New(errs.KindMalformedRequest, "Script request is malformed")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return wire, errs.New(errs.KindMalformedRequest, "Script request must contain one JSON document")
	}
	return wire, nil
}

func decodeScriptOrder(value json.RawMessage) (*uint16, error) {
	if len(value) == 0 {
		return nil, nil
	}
	var order uint16
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) || json.Unmarshal(value, &order) != nil {
		return nil, errs.New(errs.KindMalformedRequest, "Script order must be an integer from 0 through 65535")
	}
	return &order, nil
}
