package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"unicode"

	"github.com/AlanD20/groundplane/internal/common/localdiag"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: same-host key inspection must bypass malformed human CLI config
// while rendering only the approved safe projection returned by the app port.
func TestControllerKeyShowUsesInjectedLocalInspector(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "invalid-cli.yaml")
	if err := os.WriteFile(configPath, []byte("unknown: true\n"), 0o600); err != nil {
		t.Fatalf("write CLI config: %v", err)
	}

	var output bytes.Buffer
	called := false
	root := NewRootCmd(Dependencies{InspectControllerKey: func(context.Context) (localdiag.ControllerKey, error) {
		called = true
		return localdiag.ControllerKey{
			Path:        "/etc/groundplane/controller.age",
			Fingerprint: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}, nil
	}})
	root.SetOut(&output)
	root.SetArgs([]string{
		"--config", configPath,
		"--host", "://not-a-rest-address",
		"--output", "JSON",
		"controller", "key", "show",
	})
	if err := root.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("controller key show: %v", err)
	}
	if !called || strings.Contains(output.String(), "unknown") ||
		!strings.Contains(output.String(), `"fingerprint": "sha256:aaaaaaaa`) {
		t.Fatalf("called/output = %v/%q", called, output.String())
	}
}

// Rationale: one unreachable etcd peer must remain visible beside healthy
// peers instead of collapsing the local diagnostic into an all-or-nothing API.
func TestControllerEtcdShowRendersPartialResultsBeforeError(t *testing.T) {
	var output bytes.Buffer
	root := NewRootCmd(Dependencies{InspectControllerEtcd: func(context.Context) ([]localdiag.EtcdEndpoint, error) {
		return []localdiag.EtcdEndpoint{
			{
				Endpoint: "10.0.0.1:2379", Healthy: true, Version: "3.6.13", MemberID: "11",
				LeaderID: "11", Revision: "29", DBSizeBytes: cliInt64Pointer(4096), LatencyMS: cliInt64Pointer(7),
			},
			{Endpoint: "10.0.0.2:2379", Error: "endpoint unavailable"},
		}, errs.New(errs.KindStorageUnavailable, "one or more etcd endpoints are unavailable")
	}})
	root.SetOut(&output)
	configPath := filepath.Join(t.TempDir(), "invalid-cli.yaml")
	if err := os.WriteFile(configPath, []byte("unknown: true\n"), 0o600); err != nil {
		t.Fatalf("write CLI config: %v", err)
	}
	root.SetArgs([]string{
		"--config", configPath,
		"--host", "://not-a-rest-address",
		"--no-color",
		"controller", "etcd", "show",
	})
	err := root.ExecuteContext(context.Background())
	if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("controller etcd show error = %v", err)
	}
	for _, value := range []string{
		"ENDPOINT", "HEALTH", "VERSION", "MEMBER", "LEADER", "REVISION", "DB_SIZE", "LATENCY", "ERROR",
		"10.0.0.1:2379", "true", "3.6.13", "11", "29", "4096", "7",
		"10.0.0.2:2379", "false", "endpoint unavailable",
	} {
		if !strings.Contains(output.String(), value) {
			t.Fatalf("output %q does not contain %q", output.String(), value)
		}
	}
}

// Rationale: key diagnostics must keep one exact safe projection in every
// supported output format, with the approved TABLE headers in approved order.
func TestControllerKeyShowFormats(t *testing.T) {
	result := localdiag.ControllerKey{
		Path:        "/etc/groundplane/controller.age",
		Fingerprint: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for _, test := range []struct {
		name   string
		format string
		want   string
	}{
		{name: "json", format: "JSON", want: "{\n  \"path\": \"/etc/groundplane/controller.age\",\n  \"fingerprint\": \"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\n}\n"},
		{name: "yaml", format: "YAML", want: "path: /etc/groundplane/controller.age\nfingerprint: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			output, err := executeControllerKeyShow(t, test.format, result)
			if err != nil || output != test.want {
				t.Fatalf("controller key show = %q, %v; want %q", output, err, test.want)
			}
		})
	}
	output, err := executeControllerKeyShow(t, "TABLE", result)
	if err != nil || !reflect.DeepEqual(tableHeaders(output), []string{"PATH", "FINGERPRINT"}) {
		t.Fatalf("controller key TABLE = %q, %v; headers=%#v", output, err, tableHeaders(output))
	}
}

// Rationale: healthy zero-valued observations are valid data in structured
// formats, while unavailable rows must omit every status-only field.
func TestControllerEtcdShowFormats(t *testing.T) {
	zero := int64(0)
	results := []localdiag.EtcdEndpoint{
		{
			Endpoint: "10.0.0.1:2379", Healthy: true, Version: "3.6.13", MemberID: "1", LeaderID: "1",
			Revision: "0", DBSizeBytes: &zero, LatencyMS: &zero,
		},
		{Endpoint: "10.0.0.2:2379", Error: "endpoint unavailable"},
	}
	for _, test := range []struct {
		name   string
		format string
		want   string
	}{
		{name: "json", format: "JSON", want: "[\n  {\n    \"endpoint\": \"10.0.0.1:2379\",\n    \"healthy\": true,\n    \"version\": \"3.6.13\",\n    \"member_id\": \"1\",\n    \"leader_id\": \"1\",\n    \"revision\": \"0\",\n    \"db_size_bytes\": 0,\n    \"latency_ms\": 0\n  },\n  {\n    \"endpoint\": \"10.0.0.2:2379\",\n    \"healthy\": false,\n    \"error\": \"endpoint unavailable\"\n  }\n]\n"},
		{name: "yaml", format: "YAML", want: "- endpoint: 10.0.0.1:2379\n  healthy: true\n  version: 3.6.13\n  member_id: \"1\"\n  leader_id: \"1\"\n  revision: \"0\"\n  db_size_bytes: 0\n  latency_ms: 0\n- endpoint: 10.0.0.2:2379\n  healthy: false\n  error: endpoint unavailable\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			output, err := executeControllerEtcdShow(t, test.format, results)
			if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) || output != test.want {
				t.Fatalf("controller etcd show = %q, %v; want %q", output, err, test.want)
			}
		})
	}
	output, err := executeControllerEtcdShow(t, "TABLE", results)
	wantHeaders := []string{
		"ENDPOINT",
		"HEALTH",
		"VERSION",
		"MEMBER",
		"LEADER",
		"REVISION",
		"DB_SIZE",
		"LATENCY",
		"ERROR",
	}
	if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) ||
		!reflect.DeepEqual(tableHeaders(output), wantHeaders) {
		t.Fatalf("controller etcd TABLE = %q, %v; headers=%#v", output, err, tableHeaders(output))
	}
}

func executeControllerKeyShow(t *testing.T, format string, result localdiag.ControllerKey) (string, error) {
	t.Helper()
	var output bytes.Buffer
	root := NewRootCmd(Dependencies{InspectControllerKey: func(context.Context) (localdiag.ControllerKey, error) {
		return result, nil
	}})
	root.SetOut(&output)
	root.SetArgs([]string{"--no-color", "--output", format, "controller", "key", "show"})
	err := root.ExecuteContext(context.Background())
	return output.String(), err
}

func executeControllerEtcdShow(
	t *testing.T,
	format string,
	results []localdiag.EtcdEndpoint,
) (string, error) {
	t.Helper()
	var output bytes.Buffer
	root := NewRootCmd(Dependencies{InspectControllerEtcd: func(context.Context) ([]localdiag.EtcdEndpoint, error) {
		return results, errs.New(errs.KindStorageUnavailable, "unavailable")
	}})
	root.SetOut(&output)
	root.SetArgs([]string{"--no-color", "--output", format, "controller", "etcd", "show"})
	err := root.ExecuteContext(context.Background())
	return output.String(), err
}

func tableHeaders(output string) []string {
	for _, line := range strings.Split(output, "\n") {
		if !strings.Contains(line, "ENDPOINT") && !strings.Contains(line, "PATH") {
			continue
		}
		return strings.FieldsFunc(line, func(value rune) bool {
			return value == '|' || unicode.IsSpace(value)
		})
	}
	return nil
}

func cliInt64Pointer(value int64) *int64 {
	return &value
}
