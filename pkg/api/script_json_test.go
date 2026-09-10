package api

import (
	"encoding/json"
	"testing"
)

// Rationale: custom decoding must retain existing fields and distinguish the
// creation default, an omitted patch and a deliberate reset to zero.
func TestScriptJSONPreservesFieldsAndOrderPresence(t *testing.T) {
	var created ScriptCreate
	if err := json.Unmarshal([]byte(`{"environment_id":"env","service_id":"svc",`+
		`"slug":"migrate","script":"echo migrate","when":"pre-deploy"}`), &created); err != nil {
		t.Fatal(err)
	}
	if created.EnvironmentID != "env" || created.ServiceID != "svc" || created.Slug != "migrate" ||
		created.Body != "echo migrate" || created.When != "pre-deploy" || created.Order != 0 {
		t.Fatalf("create fields = %#v", created)
	}
	for _, test := range []struct {
		body    string
		present bool
		order   uint16
	}{
		{`{"slug":"migrate"}`, false, 0},
		{`{"slug":"migrate","order":0}`, true, 0},
		{`{"slug":"migrate","order":65535}`, true, 65535},
	} {
		var edited ScriptEdit
		if err := json.Unmarshal([]byte(test.body), &edited); err != nil {
			t.Fatal(err)
		}
		if edited.Slug == nil || *edited.Slug != "migrate" || (edited.Order != nil) != test.present ||
			(test.present && *edited.Order != test.order) {
			t.Fatalf("edit %s = %#v", test.body, edited)
		}
	}
}

// Rationale: schema bypasses and direct callers receive the same closed JSON
// boundary; malformed requests never acquire an accidentally coerced order.
func TestScriptJSONRejectsAmbiguousAndMalformedRequests(t *testing.T) {
	for _, body := range []string{
		`null`, `[]`, `{"unknown":1}`, `{"order":1,"order":2}`, `{"order":0} {}`,
		`{"order":null}`, `{"order":-1}`, `{"order":65536}`, `{"order":1.0}`,
		`{"order":1.5}`, `{"order":"10"}`, `{"order":true}`, `{"order":[]}`, `{"order":{}}`,
	} {
		t.Run(body, func(t *testing.T) {
			var created ScriptCreate
			if err := json.Unmarshal([]byte(body), &created); err == nil {
				t.Fatal("create accepted malformed input")
			}
			var edited ScriptEdit
			if err := json.Unmarshal([]byte(body), &edited); err == nil {
				t.Fatal("edit accepted malformed input")
			}
		})
	}
}
