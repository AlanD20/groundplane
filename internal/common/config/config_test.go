package config

import "testing"

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

func TestConfigValidateKeepsAgentChannelAddressGeneric(t *testing.T) {
	t.Parallel()

	controller := DefaultControllerConfig()
	controller.Listen.GRPC = "0.0.0.0:8081"
	if err := controller.Validate(); err != nil {
		t.Fatalf("ControllerConfig.Validate() error = %v, want wildcard Agent-channel listener allowed", err)
	}

	agent := DefaultAgentConfig()
	agent.Controller.Address = "[::1]:8081"
	if err := agent.Validate(); err != nil {
		t.Fatalf("AgentConfig.Validate() error = %v, want IPv6 Agent-channel address allowed", err)
	}
}
