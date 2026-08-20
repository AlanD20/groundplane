package api

import (
	"encoding/json"
	"testing"
)

func TestServiceRuntimeIntentJSON(t *testing.T) {
	// Rationale: Service responses must expose runtime_intent with exact
	// snake_case JSON while keeping it out of Blueprint input.
	encoded, err := json.Marshal(Service{
		ID:            "svc_x",
		Name:          "api",
		Image:         "app:latest",
		RuntimeIntent: ServiceRuntimeIntentStopped,
	})
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	want := `{"id":"svc_x","name":"api","image":"app:latest","runtime_intent":"stopped"}`
	if string(encoded) != want {
		t.Fatalf("Marshal() = %s, want %s", encoded, want)
	}
}
