package api

import (
	"encoding/json"
	"testing"
)

func TestReleaseGroupOnFailureJSON(t *testing.T) {
	// Rationale: the public response must expose the canonical snake_case field
	// and enum spelling consumed identically by Console and CLI.
	encoded, err := json.Marshal(ReleaseGroup{
		ID:        "rg_x",
		Name:      "realtime",
		Services:  []string{"api", "worker"},
		OnFailure: OnFailureSwitchBack,
	})
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	want := `{"id":"rg_x","name":"realtime","services":["api","worker"],"on_failure":"switch_back"}`
	if string(encoded) != want {
		t.Fatalf("Marshal() = %s, want %s", encoded, want)
	}
}
