package app

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/localdiag"
)

// Rationale: key diagnostics need one canonical, grep-safe fingerprint format
// and must hash the public recipient rather than secret identity bytes.
func TestInspectControllerKeyFingerprintsCanonicalRecipient(t *testing.T) {
	info, err := inspectControllerKey(
		context.Background(),
		"/etc/groundplane/controller.age",
		func(context.Context, string) (string, error) { return "abc", nil },
	)
	if err != nil {
		t.Fatalf("inspectControllerKey() error = %v", err)
	}
	const want = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if info.Path != "/etc/groundplane/controller.age" || info.Fingerprint != want {
		t.Fatalf("inspectControllerKey() = %#v", info)
	}
}

// Rationale: daemon startup and local diagnostics must resolve the same
// Controller config path rather than drifting into separate defaults.
func TestControllerConfigPathUsesDefaultAndEnvironmentOverride(t *testing.T) {
	t.Setenv("GROUNDPLANE_CONTROLLER_CONFIG", "")
	if got := ControllerConfigPath(); got != DefaultControllerConfigPath {
		t.Fatalf("ControllerConfigPath() = %q, want %q", got, DefaultControllerConfigPath)
	}
	t.Setenv("GROUNDPLANE_CONTROLLER_CONFIG", "/custom/controller.yaml")
	if got := ControllerConfigPath(); got != "/custom/controller.yaml" {
		t.Fatalf("ControllerConfigPath() = %q", got)
	}
}

// Rationale: both diagnostics must load strict daemon YAML and forward its
// exact key path and configured endpoint order through their app ports.
func TestControllerDiagnosticsLoadAndForwardControllerConfig(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "controller.yaml")
	contents := []byte(
		"environment_pool: 10.0.0.0/9\nsystem_pool: 10.128.0.0/9\n" +
			"etcd:\n  endpoints: [10.0.0.2:2379, 10.0.0.1:2379]\nage_key_path: /custom/controller.age\n",
	)
	if err := os.WriteFile(configPath, contents, 0o600); err != nil {
		t.Fatalf("write Controller config: %v", err)
	}

	keyInfo, err := inspectControllerKeyFromConfig(
		context.Background(),
		configPath,
		func(_ context.Context, path string) (string, error) {
			if path != "/custom/controller.age" {
				t.Fatalf("key path = %q", path)
			}
			return "abc", nil
		},
	)
	if err != nil || keyInfo.Path != "/custom/controller.age" {
		t.Fatalf("inspectControllerKeyFromConfig() = %#v, %v", keyInfo, err)
	}

	wantEndpoints := []string{"10.0.0.2:2379", "10.0.0.1:2379"}
	rows, err := inspectControllerEtcdFromConfig(
		context.Background(),
		configPath,
		func(_ context.Context, endpoints []string) ([]localdiag.EtcdEndpoint, error) {
			if !reflect.DeepEqual(endpoints, wantEndpoints) {
				t.Fatalf("endpoints = %#v, want %#v", endpoints, wantEndpoints)
			}
			return []localdiag.EtcdEndpoint{{Endpoint: endpoints[0], Healthy: true}}, nil
		},
	)
	if err != nil || len(rows) != 1 || rows[0].Endpoint != wantEndpoints[0] {
		t.Fatalf("inspectControllerEtcdFromConfig() = %#v, %v", rows, err)
	}
}

// Rationale: strict Controller validation must fail before either diagnostic
// touches its infrastructure dependency.
func TestControllerDiagnosticsRejectInvalidConfigBeforeForwarding(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "controller.yaml")
	if err := os.WriteFile(configPath, []byte("etcd:\n  endpoints: []\n"), 0o600); err != nil {
		t.Fatalf("write Controller config: %v", err)
	}

	called := false
	_, err := inspectControllerEtcdFromConfig(
		context.Background(),
		configPath,
		func(context.Context, []string) ([]localdiag.EtcdEndpoint, error) {
			called = true
			return nil, nil
		},
	)
	if err == nil || called {
		t.Fatalf("inspectControllerEtcdFromConfig() error/called = %v/%v", err, called)
	}
}
