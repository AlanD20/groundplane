package api

import (
	"encoding/json"
	"testing"
)

// QA: GRP-01, UI-01 - L0 response-model encoding only; no Controller read,
// Console/CLI rendering, persisted default, or group execution is exercised.
// Rationale: the public response must expose the canonical snake_case field
// and enum spelling so clients do not lose or misread the compensation policy.
func TestReleaseGroupOnFailureJSON(t *testing.T) {
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

// QA: GRP-01, UI-03, UI-04 - L0 request decoding only; no HTTP handler,
// idempotency record, group publication, or Service mutation is exercised.
// Rationale: duplicate, unknown, or trailing JSON must not admit two wire
// representations whose last decoded field produces the same mutation digest.
func TestReleaseGroupRequestsRejectAmbiguousJSON(t *testing.T) {
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

// QA: GRP-01, UI-03 - L0 PATCH-model decoding only; no existing group,
// Controller edit, fixed-revision publication, or later deploy is exercised.
// Rationale: PATCH must distinguish omission (preserve), null (clear), and
// string (replace); collapsing any pair silently changes desired state.
func TestReleaseGroupEditPreservesTagPresence(t *testing.T) {
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

// QA: GRP-01, UI-03, UI-04 - L0 PATCH-model rejection only; no HTTP response,
// protected-intent replay, or durable edit is exercised.
// Rationale: duplicate PATCH fields make field-presence semantics dependent
// on decoder overwrite order and therefore cannot be idempotently hashed.
func TestReleaseGroupEditRejectsDuplicatePatchField(t *testing.T) {
	t.Parallel()

	var request ReleaseGroupEditRequest
	if err := json.Unmarshal([]byte(`{"tag":null,"tag":"sha-123"}`), &request); err == nil {
		t.Fatal("Unmarshal() error = nil, want duplicate-field failure")
	}
}

// QA: GRP-01, UI-01 - L0 PATCH-model encoding only; no generated-client
// transport, Controller interpretation, stored group, or deploy is exercised.
// Rationale: omission and explicit null must serialize distinctly rather than
// exposing the helper struct or turning a preserved tag into a clear operation.
func TestReleaseGroupEditMarshalPreservesFieldPresence(t *testing.T) {
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
