package api

import "encoding/json"

// Omission means reuse dormant desired config. Explicit null is not omission:
// accepting it could enable a different placement than the caller intended.
func (request *ComponentEnableRequest) UnmarshalJSON(data []byte) error {
	if err := rejectDuplicateJSONMembers(data); err != nil {
		return malformedComponentConfig("component enable request contains invalid or duplicate members")
	}
	var wire struct {
		Config json.RawMessage `json:"config"`
	}
	if err := decodeComponentConfigWire(data, &wire); err != nil {
		return err
	}
	if !present(wire.Config) {
		*request = ComponentEnableRequest{}
		return nil
	}
	var config ComponentConfigMutationInput
	if err := decodeRequiredField(wire.Config, "config", &config); err != nil {
		return err
	}
	*request = ComponentEnableRequest{Config: &config}
	return nil
}
