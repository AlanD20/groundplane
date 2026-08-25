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

// Rationale: the public maximum must survive request decoding and response
// encoding without losing integer fidelity in any supported JSON consumer.
func TestBackupPolicyKeepJSONPreservesMaximumPublicValue(t *testing.T) {
	t.Parallel()
	const body = `{"enabled":false,"keep":9007199254740991,"sources":[]}`
	var request BackupPolicyReplacementRequest
	if err := json.Unmarshal([]byte(body), &request); err != nil {
		t.Fatalf("json.Unmarshal(maximum Keep) error = %v", err)
	}
	if request.Keep != MaximumBackupPolicyKeep {
		t.Fatalf("request Keep = %d, want %d", request.Keep, MaximumBackupPolicyKeep)
	}
	encoded, err := json.Marshal(BackupPolicy{Keep: request.Keep, Sources: []BackupSource{}})
	if err != nil {
		t.Fatalf("json.Marshal(maximum Keep) error = %v", err)
	}
	if string(encoded) != body {
		t.Fatalf("BackupPolicy JSON = %s", encoded)
	}
}

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
		{keep: MaximumBackupPolicyKeep, want: true},
		{keep: MaximumBackupPolicyKeep + 1, want: false},
	}
	for _, test := range tests {
		if got := ValidBackupPolicyKeep(test.keep); got != test.want {
			t.Fatalf("ValidBackupPolicyKeep(%d) = %t, want %t", test.keep, got, test.want)
		}
	}
}

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
