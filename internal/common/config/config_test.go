package config

import "testing"

// Rationale: the human API must remain loopback-only with a concrete numeric port.
func TestControllerConfigValidateHumanHTTPListener(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		address string
		wantErr bool
	}{
		{name: "default loopback address", address: "127.0.0.1:8080"},
		{name: "lowest valid port", address: "127.0.0.1:1"},
		{name: "highest valid port", address: "127.0.0.1:65535"},
		{name: "empty host", address: ":8080", wantErr: true},
		{name: "wildcard IPv4", address: "0.0.0.0:8080", wantErr: true},
		{name: "public IPv4", address: "203.0.113.10:8080", wantErr: true},
		{name: "private IPv4", address: "192.168.1.10:8080", wantErr: true},
		{name: "localhost name", address: "localhost:8080", wantErr: true},
		{name: "IPv6 loopback", address: "[::1]:8080", wantErr: true},
		{name: "empty port", address: "127.0.0.1:", wantErr: true},
		{name: "named port", address: "127.0.0.1:http", wantErr: true},
		{name: "zero port", address: "127.0.0.1:0", wantErr: true},
		{name: "port above range", address: "127.0.0.1:65536", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cfg := DefaultControllerConfig()
			cfg.Listen.HTTP = test.address
			err := cfg.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("ControllerConfig.Validate() error = %v, want error = %t", err, test.wantErr)
			}
		})
	}
}

// Rationale: a missing or noncanonical runtime identity must fail before the
// Agent can authenticate or accept work.
func TestAgentConfigRequiresCanonicalAgentID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		agentID string
		wantErr bool
	}{
		{name: "canonical", agentID: "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{name: "missing", wantErr: true},
		{name: "wrong kind", agentID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", wantErr: true},
		{name: "lowercase", agentID: "agt_01arz3ndektsv4rrffq69g5fav", wantErr: true},
		{name: "malformed", agentID: "agt_not-an-id", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := DefaultAgentConfig()
			cfg.AgentID = test.agentID
			err := cfg.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("AgentConfig.Validate() error = %v, want error = %t", err, test.wantErr)
			}
		})
	}
}
