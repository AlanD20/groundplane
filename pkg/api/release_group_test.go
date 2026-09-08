package api

import (
	"encoding/json"
	"testing"
)

func TestReleaseGroupOnFailureJSON(t *testing.T) {
	// Rationale: the public response must expose the canonical snake_case field
	// and enum spelling consumed identically by Console and CLI.
	encoded, err := json.Marshal(ReleaseGroup{
		ID:            "rg_x",
		EnvironmentID: "env_x",
		Name:          "realtime",
		ServiceIDs:    []string{"svc_api", "svc_worker"},
		Order:         []string{"svc_api", "svc_worker"},
		OnFailure:     OnFailureSwitchBack,
	})
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	want := `{"id":"rg_x","environment_id":"env_x","name":"realtime","service_ids":["svc_api","svc_worker"],"order":["svc_api","svc_worker"],"on_failure":"switch_back"}`
	if string(encoded) != want {
		t.Fatalf("Marshal() = %s, want %s", encoded, want)
	}
}

func TestReleaseGroupRequestsRejectAmbiguousJSON(t *testing.T) {
	// Rationale: idempotency digests are computed after decoding, so duplicate,
	// unknown, or trailing JSON must not admit two wire representations whose
	// last decoded field happens to produce the same mutation.
	t.Parallel()

	for _, document := range []string{
		`{"environment_id":"env_a","environment_id":"env_b","name":"g","service_ids":["a","b"],"order":["a","b"]}`,
		`{"environment_id":"env_a","name":"g","service_ids":["a","b"],"order":["a","b"],"unexpected":true}`,
		`{"environment_id":"env_a","name":"g","service_ids":["a","b"],"order":["a","b"]} {}`,
	} {
		var request ReleaseGroupAddRequest
		if err := json.Unmarshal([]byte(document), &request); err == nil {
			t.Fatalf("Unmarshal(%s) error = nil, want strict JSON failure", document)
		}
	}
}

func TestReleaseGroupEditPreservesTagPresence(t *testing.T) {
	// Rationale: PATCH must distinguish omission (preserve), null (clear), and
	// string (replace); collapsing any pair silently changes desired state.
	t.Parallel()

	tests := []struct {
		document string
		present  bool
		value    *string
	}{
		{document: `{}`},
		{document: `{"tag":null}`, present: true},
		{document: `{"tag":"sha-123"}`, present: true, value: stringPointer("sha-123")},
	}
	for _, test := range tests {
		var request ReleaseGroupEditRequest
		if err := json.Unmarshal([]byte(test.document), &request); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", test.document, err)
		}
		if request.Tag.Present != test.present {
			t.Fatalf("Unmarshal(%s) present = %v, want %v", test.document, request.Tag.Present, test.present)
		}
		if (request.Tag.Value == nil) != (test.value == nil) ||
			request.Tag.Value != nil && *request.Tag.Value != *test.value {
			t.Fatalf("Unmarshal(%s) value = %v, want %v", test.document, request.Tag.Value, test.value)
		}
	}
}

func TestReleaseGroupEditRejectsDuplicatePatchField(t *testing.T) {
	// Rationale: duplicate PATCH fields make field-presence semantics dependent
	// on decoder overwrite order and therefore cannot be idempotently hashed.
	t.Parallel()

	var request ReleaseGroupEditRequest
	if err := json.Unmarshal([]byte(`{"tag":null,"tag":"sha-123"}`), &request); err == nil {
		t.Fatal("Unmarshal() error = nil, want duplicate-field failure")
	}
}

func TestReleaseGroupEditMarshalPreservesFieldPresence(t *testing.T) {
	// Rationale: generated clients bridge through JSON; omission and explicit
	// null must survive that bridge exactly rather than serializing the helper
	// struct's private representation.
	t.Parallel()

	omitted, err := json.Marshal(ReleaseGroupEditRequest{})
	if err != nil || string(omitted) != `{}` {
		t.Fatalf("Marshal(omitted) = %s, %v", omitted, err)
	}
	cleared, err := json.Marshal(ReleaseGroupEditRequest{Tag: OptionalNullableString{Present: true}})
	if err != nil || string(cleared) != `{"tag":null}` {
		t.Fatalf("Marshal(clear) = %s, %v", cleared, err)
	}
}

func stringPointer(value string) *string { return &value }
