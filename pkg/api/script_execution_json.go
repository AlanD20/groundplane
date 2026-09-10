package api

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type scriptExecutionWire struct {
	Mode     string          `json:"mode"`
	Image    json.RawMessage `json:"image"`
	User     json.RawMessage `json:"user"`
	Volumes  json.RawMessage `json:"volumes"`
	EntryIDs json.RawMessage `json:"entry_ids"`
}

type scriptVolumeGrantFields ScriptVolumeGrant

type scriptVolumeGrantWire struct {
	scriptVolumeGrantFields
	ReadOnly json.RawMessage `json:"read_only"`
}

func decodeScriptExecution(value json.RawMessage) (*ScriptExecution, error) {
	if len(value) == 0 {
		return nil, nil
	}
	var execution ScriptExecution
	if err := execution.UnmarshalJSON(value); err != nil {
		return nil, err
	}
	return &execution, nil
}

// UnmarshalJSON preserves presence until the closed mode choice is validated.
func (execution *ScriptExecution) UnmarshalJSON(value []byte) error {
	wire, err := decodeScriptRequest[scriptExecutionWire](value)
	if err != nil {
		return err
	}
	decoded := ScriptExecution{Mode: wire.Mode}
	switch wire.Mode {
	case "inherited":
		if len(wire.Image)+len(wire.User)+len(wire.Volumes)+len(wire.EntryIDs) != 0 {
			return errs.New(errs.KindMalformedRequest, "inherited Script execution accepts only mode")
		}
	case "explicit":
		if err := decodeScriptDecision(wire.Image, &decoded.Image); err != nil {
			return err
		}
		if err := decodeScriptDecision(wire.User, &decoded.User); err != nil {
			return err
		}
		if len(wire.Volumes) != 0 {
			if err := decodeScriptDecision(wire.Volumes, &decoded.Volumes); err != nil {
				return err
			}
		}
		if len(wire.EntryIDs) != 0 {
			if err := decodeScriptDecision(wire.EntryIDs, &decoded.EntryIDs); err != nil {
				return err
			}
			for _, id := range decoded.EntryIDs {
				if id == "" {
					return errs.New(errs.KindMalformedRequest, "Script execution Entry ids must be nonempty strings")
				}
			}
		}
	default:
		return errs.New(errs.KindMalformedRequest, "Script execution requires inherited or explicit mode")
	}
	*execution = decoded
	return nil
}

// UnmarshalJSON refuses an omitted or nullable access decision; false is valid.
func (grant *ScriptVolumeGrant) UnmarshalJSON(value []byte) error {
	wire, err := decodeScriptRequest[scriptVolumeGrantWire](value)
	if err != nil {
		return err
	}
	decoded := ScriptVolumeGrant(wire.scriptVolumeGrantFields)
	if err := decodeScriptDecision(wire.ReadOnly, &decoded.ReadOnly); err != nil {
		return err
	}
	*grant = decoded
	return nil
}

func decodeScriptDecision[T string | bool | []string | []ScriptVolumeGrant](value json.RawMessage, target *T) error {
	if len(value) == 0 || bytes.Equal(bytes.TrimSpace(value), []byte("null")) || json.Unmarshal(value, target) != nil {
		return errs.New(errs.KindMalformedRequest, "Script execution decision is missing or malformed")
	}
	return nil
}
