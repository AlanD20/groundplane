package api

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// OptionalNullableString distinguishes omission, JSON null, and a string.
type OptionalNullableString struct {
	Present bool
	Value   *string
}

func (value *OptionalNullableString) UnmarshalJSON(data []byte) error {
	value.Present = true
	if bytes.Equal(data, []byte("null")) {
		value.Value = nil
		return nil
	}
	var decoded string
	if err := json.Unmarshal(data, &decoded); err != nil {
		return errs.New(errs.KindMalformedRequest, "release group tag must be a string or null")
	}
	value.Value = &decoded
	return nil
}

func (value OptionalNullableString) IsZero() bool { return !value.Present }

func (request *ReleaseGroupAddRequest) UnmarshalJSON(data []byte) error {
	type plain ReleaseGroupAddRequest
	var decoded plain
	if err := decodeStrictReleaseGroupRequest(data, &decoded); err != nil {
		return err
	}
	*request = ReleaseGroupAddRequest(decoded)
	return nil
}

func (request *ReleaseGroupEditRequest) UnmarshalJSON(data []byte) error {
	type plain ReleaseGroupEditRequest
	var decoded plain
	if err := decodeStrictReleaseGroupRequest(data, &decoded); err != nil {
		return err
	}
	*request = ReleaseGroupEditRequest(decoded)
	return nil
}

func (request ReleaseGroupEditRequest) MarshalJSON() ([]byte, error) {
	fields := make(map[string]any, 5)
	if request.Name != nil {
		fields["name"] = *request.Name
	}
	if request.ServiceIDs != nil {
		fields["service_ids"] = *request.ServiceIDs
	}
	if request.Order != nil {
		fields["order"] = *request.Order
	}
	if request.Tag.Present {
		fields["tag"] = request.Tag.Value
	}
	if request.OnFailure != nil {
		fields["on_failure"] = *request.OnFailure
	}
	return json.Marshal(fields)
}

func decodeStrictReleaseGroupRequest(data []byte, destination any) error {
	if err := rejectDuplicateJSONMembers(data); err != nil {
		return errs.New(errs.KindMalformedRequest, "release group request contains duplicate or malformed JSON members")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errs.New(errs.KindMalformedRequest, "release group request is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errs.New(errs.KindMalformedRequest, "release group request must contain one JSON document")
	}
	return nil
}
