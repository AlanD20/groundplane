package api

import (
	"encoding/json"
	"testing"
)

// Rationale: policy absence is represented as an effective disabled singleton
// with an explicit empty source collection and no fabricated configuration.
func TestBackupPolicyEffectiveDisabledJSONContract(t *testing.T) {
	value, err := json.Marshal(BackupPolicy{Sources: []BackupSource{}})
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != `{"enabled":false,"sources":[]}` {
		t.Fatalf("BackupPolicy JSON = %s", value)
	}
	if MaximumBackupPolicySources != 12 {
		t.Fatalf("MaximumBackupPolicySources = %d", MaximumBackupPolicySources)
	}
}

// Rationale: direct decoding must enforce required/non-null replacement
// members and reject silent JSON widening before application idempotency.
func TestBackupPolicyReplacementStrictJSON(t *testing.T) {
	invalid := []string{
		`{}`,
		`null`,
		`{"enabled":false,"sources":null}`,
		`{"enabled":false,"sources":[],"unknown":true}`,
		`{"enabled":false,"enabled":true,"sources":[]}`,
		`{"enabled":false,"sources":[],"frequency":null}`,
	}
	for _, body := range invalid {
		var request BackupPolicyReplacementRequest
		if err := json.Unmarshal([]byte(body), &request); err == nil {
			t.Fatalf("json.Unmarshal(%s) error = nil", body)
		}
	}
	var request BackupPolicyReplacementRequest
	if err := json.Unmarshal([]byte(`{"enabled":false,"sources":[]}`), &request); err != nil {
		t.Fatalf("json.Unmarshal(valid) error = %v", err)
	}
	if request.Enabled || request.Sources == nil || len(request.Sources) != 0 {
		t.Fatalf("request = %#v", request)
	}
}
