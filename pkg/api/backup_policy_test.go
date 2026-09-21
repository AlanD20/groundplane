package api

import (
	"encoding/json"
	"testing"
)

// QA: BAK-01 - L0 public-model proof only; no Controller validation, durable
// policy publication, scheduling, source resolution, or backup effect.
// Rationale: policy absence must keep an explicit empty source collection and
// must not fabricate configuration fields that an operator never supplied.
func TestBackupPolicyEffectiveDisabledJSONContract(t *testing.T) {
	value, err := json.Marshal(BackupPolicy{Sources: []BackupSource{}})
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != `{"enabled":false,"sources":[],"next_run_at":null}` {
		t.Fatalf("BackupPolicy JSON = %s", value)
	}
}

// QA: BAK-01, BAK-02, UI-03 - L0 JSON decoding only; no HTTP admission,
// semantic policy validation, idempotency record, or durable write is exercised.
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

// QA: BAK-01, UI-03 - L0 JSON fidelity only; no Controller range validation,
// retention pruning, generated client, or persistence boundary is exercised.
// Rationale: the public maximum must survive request decoding and response
// encoding without losing integer fidelity in any supported JSON consumer.
func TestBackupPolicyKeepJSONPreservesMaximumPublicValue(t *testing.T) {
	t.Parallel()
	const body = `{"enabled":false,"keep":9007199254740991,"sources":[]}`
	var request BackupPolicyReplacementRequest
	if err := json.Unmarshal([]byte(body), &request); err != nil {
		t.Fatalf("json.Unmarshal(maximum Keep) error = %v", err)
	}
	if request.Keep != 9_007_199_254_740_991 {
		t.Fatalf("request Keep = %d, want %d", request.Keep, int64(9_007_199_254_740_991))
	}
	encoded, err := json.Marshal(BackupPolicy{Keep: request.Keep, Sources: []BackupSource{}})
	if err != nil {
		t.Fatalf("json.Marshal(maximum Keep) error = %v", err)
	}
	if string(encoded) != `{"enabled":false,"keep":9007199254740991,"sources":[],"next_run_at":null}` {
		t.Fatalf("BackupPolicy JSON = %s", encoded)
	}
}

// QA: BAK-01, UI-03 - L0 predicate proof only; no request handler, enabled-policy
// completeness validation, or retention behavior is exercised.
// Rationale: semantic validation uses the exact published interval rather
// than a platform-dependent machine integer range.
func TestValidBackupPolicyKeepBoundaries(t *testing.T) {
	t.Parallel()
	tests := []struct {
		keep int64
		want bool
	}{
		{keep: 0, want: false},
		{keep: 1, want: true},
		{keep: 9_007_199_254_740_991, want: true},
		{keep: 9_007_199_254_740_992, want: false},
	}
	for _, test := range tests {
		if got := ValidBackupPolicyKeep(test.keep); got != test.want {
			t.Fatalf("ValidBackupPolicyKeep(%d) = %t, want %t", test.keep, got, test.want)
		}
	}
}

// QA: BAK-01, UI-03 - L0 decoder proof only; no HTTP problem mapping or durable
// policy-state assertion is exercised.
// Rationale: values outside the signed 64-bit API contract must be rejected
// as malformed instead of wrapping or being rounded through a float.
func TestBackupPolicyKeepJSONRejectsInt64Overflow(t *testing.T) {
	t.Parallel()
	const body = `{"enabled":false,"keep":9223372036854775808,"sources":[]}`
	var request BackupPolicyReplacementRequest
	if err := json.Unmarshal([]byte(body), &request); err == nil {
		t.Fatal("json.Unmarshal(overflow Keep) error = nil")
	}
}
