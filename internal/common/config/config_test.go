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

			cfg := validControllerConfig()
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
			cfg.Runtime.PullIntervalSeconds = 2
			cfg.Runtime.MaxConcurrentTasks = 3
			err := cfg.Validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("AgentConfig.Validate() error = %v, want error = %t", err, test.wantErr)
			}
		})
	}
}

func TestControllerAgentBootstrapDefaultsAndImageValidation(t *testing.T) {
	t.Parallel()

	defaults := validControllerConfig()
	if defaults.Agent.Image != "" || defaults.Agent.Runtime.PullIntervalSeconds != 2 ||
		defaults.Agent.Runtime.MaxConcurrentTasks != 3 || len(defaults.Agent.Runtime.Labels) != 0 {
		t.Fatalf("default Agent bootstrap = %#v", defaults.Agent)
	}

	tagged := defaults
	tagged.Agent.Image = "ghcr.io/aland20/groundplane-agent:latest"
	if err := tagged.Validate(); err == nil {
		t.Fatal("tagged Agent bootstrap image passed validation")
	}

	pinned := defaults
	pinned.Agent.Image = "ghcr.io/aland20/groundplane-agent@sha256:0000000000000000000000000000000000000000000000000000000000000000"
	if err := pinned.Validate(); err != nil {
		t.Fatalf("digest-pinned Agent bootstrap image error = %v", err)
	}

	invalidRuntime := defaults
	invalidRuntime.Agent.Runtime.MaxConcurrentTasks = 0
	if err := invalidRuntime.Validate(); err == nil {
		t.Fatal("invalid Agent bootstrap runtime passed validation")
	}
}

func TestControllerVolumeRootDefaultAndSyntaxValidation(t *testing.T) {
	t.Parallel()
	defaults := validControllerConfig()
	if defaults.Storage.VolumeRoot != "/var/lib/groundplane/vol" {
		t.Fatalf("default storage.volume_root = %q", defaults.Storage.VolumeRoot)
	}
	for _, root := range []string{
		"", "/", "var/lib/groundplane/vol", "/var/lib/groundplane/vol/",
		"/var/lib/../lib/groundplane/vol", "/var/lib/groundplane/vol\x00other",
	} {
		cfg := defaults
		cfg.Storage.VolumeRoot = root
		if err := cfg.Validate(); err == nil {
			t.Fatalf("storage.volume_root %q passed validation", root)
		}
	}
	custom := defaults
	custom.Storage.VolumeRoot = "/srv/groundplane-volumes"
	if err := custom.Validate(); err != nil {
		t.Fatalf("custom storage.volume_root error = %v", err)
	}
}

// Rationale: the injected runtime document must fail before the Agent starts
// when Controller-owned execution limits or labels are unusable.
func TestAgentConfigRequiresValidRuntimePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*AgentConfig)
	}{
		{name: "valid"},
		{name: "missing pull interval", mutate: func(cfg *AgentConfig) {
			cfg.Runtime.PullIntervalSeconds = 0
		}},
		{name: "missing concurrency", mutate: func(cfg *AgentConfig) {
			cfg.Runtime.MaxConcurrentTasks = 0
		}},
		{name: "invalid label", mutate: func(cfg *AgentConfig) {
			cfg.Runtime.Labels = map[string]string{"role": "worker\x00admin"}
		}},
		{name: "invalid volume root", mutate: func(cfg *AgentConfig) {
			cfg.Storage.VolumeRoot = "/"
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := validAgentConfig()
			if test.mutate != nil {
				test.mutate(&cfg)
			}
			err := cfg.Validate()
			if (err != nil) != (test.mutate != nil) {
				t.Fatalf("AgentConfig.Validate() error = %v, want error = %t", err, test.mutate != nil)
			}
		})
	}
}

func validAgentConfig() AgentConfig {
	cfg := DefaultAgentConfig()
	cfg.AgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	cfg.Runtime.PullIntervalSeconds = 2
	cfg.Runtime.MaxConcurrentTasks = 3
	cfg.Runtime.Labels = map[string]string{"arch": "arm64"}
	return cfg
}

// Rationale: machine allocation roots are mandatory bootstrap decisions and
// must be canonicalized before any Controller subsystem consumes them.
func TestControllerConfigRequiresDisjointAllocationPools(t *testing.T) {
	t.Parallel()

	missing := DefaultControllerConfig()
	if err := missing.Validate(); err == nil {
		t.Fatal("missing allocation pools passed validation")
	}

	cfg := validControllerConfig()
	pools, err := cfg.AllocationPools()
	if err != nil {
		t.Fatalf("AllocationPools() error = %v", err)
	}
	if pools.Environment.String() != "10.0.0.0/9" || pools.System.String() != "10.128.0.0/9" {
		t.Fatalf("allocation pools = %#v", pools)
	}

	overlap := cfg
	overlap.SystemPool = "10.64.0.0/10"
	if err := overlap.Validate(); err == nil {
		t.Fatal("overlapping allocation roots passed validation")
	}

	ipv6 := cfg
	ipv6.EnvironmentPool = "fd00::/64"
	if err := ipv6.Validate(); err == nil {
		t.Fatal("IPv6 allocation root passed validation")
	}
}

func validControllerConfig() ControllerConfig {
	cfg := DefaultControllerConfig()
	cfg.EnvironmentPool = "10.0.0.19/9"
	cfg.SystemPool = "10.128.0.0/9"
	return cfg
}
