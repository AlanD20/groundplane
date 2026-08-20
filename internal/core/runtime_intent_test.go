package core

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestServiceRuntimeIntentValidate(t *testing.T) {
	tests := []struct {
		name    string
		intent  ServiceRuntimeIntent
		wantErr bool
	}{
		{name: "running", intent: ServiceRuntimeIntentRunning},
		{name: "stopped", intent: ServiceRuntimeIntentStopped},
		{name: "absent", intent: ServiceRuntimeIntentAbsent},
		{name: "empty", intent: "", wantErr: true},
		{name: "unknown", intent: "paused", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.intent.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestServiceRuntimeValidate(t *testing.T) {
	if err := (ServiceRuntime{
		ServiceID:     "svc_x",
		RuntimeIntent: ServiceRuntimeIntentRunning,
	}).Validate(); err != nil {
		t.Fatalf("Validate() unexpected error: %v", err)
	}

	for _, runtime := range []ServiceRuntime{
		{RuntimeIntent: ServiceRuntimeIntentRunning},
		{ServiceID: "svc_x"},
		{ServiceID: "svc_x", RuntimeIntent: "paused"},
	} {
		if err := runtime.Validate(); err == nil {
			t.Errorf("Validate() with %#v: expected error", runtime)
		}
	}
}

func TestDesiredServiceDoesNotContainRuntimeIntent(t *testing.T) {
	encoded, err := json.Marshal(Service{ID: "svc_x", Name: "api", Image: "app:latest"})
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}
	if strings.Contains(string(encoded), "runtime_intent") {
		t.Fatalf("desired Service unexpectedly contains runtime_intent: %s", encoded)
	}
}
