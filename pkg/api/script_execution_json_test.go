package api

import (
	"encoding/json"
	"strings"
	"testing"
)

// QA: SCRIPT-01, SCRIPT-05, UI-03 - L0 JSON-model proof only; no image sealing,
// grant authorization, Task capture, container isolation, or mount effect is proved.
// Rationale: a complete context survives decoding and re-encoding, including an
// authored false grant; omission and an inherited reset remain different patches.
func TestScriptExecutionJSONRoundTrip(t *testing.T) {
	for _, body := range []string{
		`{"execution":{"mode":"inherited"}}`,
		`{"execution":{"mode":"explicit","image":"setup@sha256:` + strings.Repeat("a", 64) +
			`","user":"0:0","volumes":[{"volume_id":"volume","target":"/etc/tls","read_only":false}],"entry_ids":["entry"]}}`,
	} {
		t.Run(body, func(t *testing.T) {
			var edited ScriptEdit
			if err := json.Unmarshal([]byte(body), &edited); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(edited)
			if err != nil || string(encoded) != body {
				t.Fatalf("edit round trip = %s, %v; want %s", encoded, err, body)
			}
			var created ScriptCreate
			if err := json.Unmarshal([]byte(body), &created); err != nil {
				t.Fatal(err)
			}
			if created.Execution == nil {
				t.Fatal("create discarded execution choice")
			}
			createdExecution, err := json.Marshal(created.Execution)
			if err != nil {
				t.Fatalf("marshal create execution: %v", err)
			}
			wantExecution := body[len(`{"execution":`) : len(body)-1]
			if string(createdExecution) != wantExecution {
				t.Fatalf("create execution = %s, want %s", createdExecution, wantExecution)
			}
		})
	}
}

// QA: SCRIPT-01, SCRIPT-06, UI-03 - L0 decoder rejection only; no Controller
// semantic validation, source lookup, Task publication, or runner isolation is proved.
// Rationale: JSON null, missing decisions, unknown members and duplicate keys
// must not widen access or be treated as an omitted context.
func TestScriptExecutionJSONRejectsAmbiguousChoices(t *testing.T) {
	for _, execution := range []string{
		`null`, `[]`, `{}`, `{"mode":"other"}`, `{"mode":null}`,
		`{"mode":"inherited","image":""}`, `{"mode":"inherited","user":null}`,
		`{"mode":"inherited","volumes":[]}`, `{"mode":"inherited","entry_ids":null}`,
		`{"mode":"inherited","unknown":true}`, `{"mode":"inherited","mode":"inherited"}`,
		`{"mode":"explicit","user":"0:0"}`, `{"mode":"explicit","image":"image"}`,
		`{"mode":"explicit","image":null,"user":"0:0"}`,
		`{"mode":"explicit","image":"image","user":0}`,
		`{"mode":"explicit","image":"image","user":"0:0","volumes":null}`,
		`{"mode":"explicit","image":"image","user":"0:0","entry_ids":null}`,
		`{"mode":"explicit","image":"image","user":"0:0","volumes":[null]}`,
		`{"mode":"explicit","image":"image","user":"0:0","volumes":[{"volume_id":"v","target":"/data"}]}`,
		`{"mode":"explicit","image":"image","user":"0:0","volumes":[{"volume_id":"v","target":"/data","read_only":null}]}`,
		`{"mode":"explicit","image":"image","user":"0:0","volumes":[{"volume_id":"v","target":"/data","read_only":"false"}]}`,
		`{"mode":"explicit","image":"image","user":"0:0","volumes":[{"volume_id":"v","target":"/data","read_only":false,"extra":true}]}`,
		`{"mode":"explicit","image":"image","user":"0:0","entry_ids":[null]}`,
	} {
		t.Run(execution, func(t *testing.T) {
			body := []byte(`{"execution":` + execution + `}`)
			var created ScriptCreate
			if err := json.Unmarshal(body, &created); err == nil {
				t.Fatal("create accepted ambiguous context")
			}
			var edited ScriptEdit
			if err := json.Unmarshal(body, &edited); err == nil {
				t.Fatal("edit accepted ambiguous context")
			}
		})
	}
}
