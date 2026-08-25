package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

const (
	backupPolicyTenantID          = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	backupPolicyProjectID         = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	backupPolicyTestEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	backupPolicyConnectorID       = "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	backupPolicyAttachID          = "att_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	backupPolicyVolumeID          = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: source order is part of the replacement contract and each CLI
// label form must remain distinct until it is resolved to a stable id.
func TestParseBackupPolicySourcesPreservesOrder(t *testing.T) {
	t.Parallel()

	sources, err := parseBackupPolicySources([]string{"attach:database", "volume:uploads", "config"})
	if err != nil {
		t.Fatalf("parse Backup Policy sources: %v", err)
	}
	want := []backupPolicySourceReference{
		{Kind: "attach", Target: "database"},
		{Kind: "volume", Target: "uploads"},
		{Kind: "config"},
	}
	if len(sources) != len(want) {
		t.Fatalf("source count = %d, want %d", len(sources), len(want))
	}
	for index := range want {
		if sources[index] != want[index] {
			t.Fatalf("source %d = %#v, want %#v", index, sources[index], want[index])
		}
	}
}

// Rationale: malformed, duplicate, and oversized source selections must fail
// before any label-resolution requests can make a partial operator intent.
func TestParseBackupPolicySourcesRejectsInvalidSelections(t *testing.T) {
	t.Parallel()

	tooMany := make([]string, apiTypes.MaximumBackupPolicySources+1)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("attach:attach-%d", index)
	}
	tests := []struct {
		name    string
		sources []string
	}{
		{name: "empty attach label", sources: []string{"attach:"}},
		{name: "empty volume label", sources: []string{"volume:"}},
		{name: "bare attach label", sources: []string{"database"}},
		{name: "unknown prefix", sources: []string{"service:database"}},
		{name: "duplicate attach", sources: []string{"attach:database", "attach:database"}},
		{name: "duplicate config", sources: []string{"config", "config"}},
		{name: "more than twelve", sources: tooMany},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseBackupPolicySources(test.sources); err == nil {
				t.Fatal("parse Backup Policy sources unexpectedly succeeded")
			}
		})
	}
}

// Rationale: policy show is locked to the typed Environment singleton and
// must resolve the complete human scope chain before using the stable id.
func TestBackupPolicyShowResolvesEnvironmentLabel(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/tenants":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, tenantPageJSON())
		case "/api/v1/projects":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, projectPageJSON())
		case "/api/v1/environments":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, environmentPageJSON())
		case "/api/v1/environments/" + backupPolicyTestEnvironmentID + "/backup-policy":
			if request.Method != http.MethodGet {
				t.Errorf("method = %s, want GET", request.Method)
			}
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, disabledBackupPolicyJSON())
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL.String())
			http.Error(writer, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	_ = executeNoun(
		t,
		newBackupCmd(),
		server.URL,
		Scope{Tenant: "acme", Project: "storefront", Environment: "production"},
		"policy",
		"show",
	)
}

// Rationale: one policy replacement must resolve every display label through
// its live Environment collection and preserve submitted source order as ids.
func TestBackupPolicySetResolvesLabelsAndPreservesSourceOrder(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/tenants":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, tenantPageJSON())
		case "/api/v1/projects":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, projectPageJSON())
		case "/api/v1/environments":
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, environmentPageJSON())
		case "/api/v1/connectors":
			assertBackupPolicyEnvironmentQuery(t, request)
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, fmt.Sprintf(
				`{"items":[{"id":%q,"environment_id":%q,"name":"archive"}],"next_cursor":""}`,
				backupPolicyConnectorID,
				backupPolicyTestEnvironmentID,
			))
		case "/api/v1/attaches":
			assertBackupPolicyEnvironmentQuery(t, request)
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, fmt.Sprintf(
				`{"items":[{"id":%q,"name":"database"}],"next_cursor":""}`,
				backupPolicyAttachID,
			))
		case "/api/v1/volumes":
			assertBackupPolicyEnvironmentQuery(t, request)
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, fmt.Sprintf(
				`{"items":[{"id":%q,"name":"uploads"}],"next_cursor":""}`,
				backupPolicyVolumeID,
			))
		case "/api/v1/environments/" + backupPolicyTestEnvironmentID + "/backup-policy":
			assertBackupPolicySetRequest(t, request, true, true, 7)
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, configuredBackupPolicyJSON(true))
		default:
			t.Errorf("unexpected request %s %s", request.Method, request.URL.String())
			http.Error(writer, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	_ = executeNoun(
		t,
		newBackupCmd(),
		server.URL,
		Scope{Tenant: "acme", Project: "storefront", Environment: "production"},
		"policy",
		"set",
		"--connector", "archive",
		"--frequency", "Tue *-*-* 04:30:00",
		"--keep", "7",
		"--encryption", "age",
		"--source", "attach:database",
		"--source", "volume:uploads",
		"--source", "config",
	)
}

// Rationale: global --id switches every policy reference to stable-id mode;
// no label collection may be consulted in that mode.
func TestBackupPolicySetHonorsGlobalIDMode(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path != "/api/v1/environments/"+backupPolicyTestEnvironmentID+"/backup-policy" {
			t.Errorf("unexpected label-resolution request %s %s", request.Method, request.URL.String())
			http.Error(writer, "unexpected request", http.StatusNotFound)
			return
		}
		assertBackupPolicySetRequest(t, request, true, true, 7)
		writeBackupPolicyTestResponse(t, writer, http.StatusOK, configuredBackupPolicyJSON(true))
	}))
	defer server.Close()

	_ = executeNoun(
		t,
		newBackupCmd(),
		server.URL,
		Scope{Environment: backupPolicyTestEnvironmentID, AsID: true},
		"policy",
		"set",
		"--connector", backupPolicyConnectorID,
		"--frequency", "Tue *-*-* 04:30:00",
		"--keep", "7",
		"--encryption", "age",
		"--source", "attach:"+backupPolicyAttachID,
		"--source", "volume:"+backupPolicyVolumeID,
		"--source", "config",
	)
}

// Rationale: the CLI flag and generated-client dispatch preserve the largest
// retention integer represented exactly by every public JSON consumer.
func TestBackupPolicySetDispatchesMaximumPublicKeep(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path != "/api/v1/environments/"+backupPolicyTestEnvironmentID+"/backup-policy" {
			t.Errorf("unexpected request %s %s", request.Method, request.URL.String())
			http.Error(writer, "unexpected request", http.StatusNotFound)
			return
		}
		assertBackupPolicySetRequest(t, request, true, true, apiTypes.MaximumBackupPolicyKeep)
		writeBackupPolicyTestResponse(t, writer, http.StatusOK, configuredBackupPolicyJSON(true))
	}))
	defer server.Close()

	_ = executeNoun(
		t,
		newBackupCmd(),
		server.URL,
		Scope{Environment: backupPolicyTestEnvironmentID, AsID: true},
		"policy",
		"set",
		"--connector", backupPolicyConnectorID,
		"--frequency", "Tue *-*-* 04:30:00",
		"--keep", "9007199254740991",
		"--encryption", "age",
		"--source", "attach:"+backupPolicyAttachID,
		"--source", "volume:"+backupPolicyVolumeID,
		"--source", "config",
	)
}

// Rationale: a syntactically valid int64 above the public contract must fail
// before scope resolution or any HTTP request can begin.
func TestBackupPolicySetRejectsKeepAbovePublicMaximumBeforeHTTP(t *testing.T) {
	t.Parallel()
	command := newBackupCmd()
	command.SetArgs([]string{"policy", "set", "--keep", "9007199254740992"})
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), "canonical base-10 integer") {
		t.Fatalf("Execute(Keep above public maximum) error = %v, want range validation", err)
	}
}

// Rationale: the documented CLI integer grammar has one unambiguous spelling;
// Go-style hexadecimal, octal, signs, leading zeroes, and whitespace must fail
// before context resolution or HTTP dispatch.
func TestBackupPolicySetRejectsNonCanonicalKeepSyntaxBeforeHTTP(t *testing.T) {
	t.Parallel()
	for _, keep := range []string{"01", "010", "0x10", "+7", "-1", " 7", "7 "} {
		command := newBackupCmd()
		command.SetArgs([]string{"policy", "set", "--keep", keep})
		err := command.Execute()
		if err == nil || !strings.Contains(err.Error(), "canonical base-10 integer") {
			t.Fatalf("Execute(--keep %q) error = %v, want canonical syntax validation", keep, err)
		}
	}
}

// Rationale: values outside the signed 64-bit representation must fail at the
// raw decimal boundary before scope resolution or any HTTP request can begin.
func TestBackupPolicySetRejectsKeepOverflowBeforeHTTP(t *testing.T) {
	t.Parallel()
	command := newBackupCmd()
	command.SetArgs([]string{"policy", "set", "--keep", "9223372036854775808"})
	err := command.Execute()
	if err == nil || !strings.Contains(err.Error(), "canonical base-10 integer") {
		t.Fatalf("Execute(overflow Keep) error = %v, want canonical range validation", err)
	}
}

// Rationale: --off is a toggle over the singleton, not a zero-value
// replacement; every configured field and ordered source must be retained.
func TestBackupPolicySetOffRetainsConfiguredPolicy(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path != "/api/v1/environments/"+backupPolicyTestEnvironmentID+"/backup-policy" {
			t.Errorf("unexpected request %s %s", request.Method, request.URL.String())
			http.Error(writer, "unexpected request", http.StatusNotFound)
			return
		}
		switch request.Method {
		case http.MethodGet:
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, configuredBackupPolicyJSON(true))
		case http.MethodPut:
			assertBackupPolicySetRequest(t, request, false, true, 7)
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, configuredBackupPolicyJSON(false))
		default:
			t.Errorf("method = %s, want GET or PUT", request.Method)
			http.Error(writer, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	_ = executeNoun(
		t,
		newBackupCmd(),
		server.URL,
		Scope{Environment: backupPolicyTestEnvironmentID, AsID: true},
		"policy",
		"set",
		"--off",
	)
}

// Rationale: a policy that was never configured has absent optional fields;
// toggling it off must keep them absent rather than manufacture zero values.
func TestBackupPolicySetOffRetainsUnconfiguredOptionals(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path != "/api/v1/environments/"+backupPolicyTestEnvironmentID+"/backup-policy" {
			t.Errorf("unexpected request %s %s", request.Method, request.URL.String())
			http.Error(writer, "unexpected request", http.StatusNotFound)
			return
		}
		switch request.Method {
		case http.MethodGet:
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, disabledBackupPolicyJSON())
		case http.MethodPut:
			assertBackupPolicySetRequest(t, request, false, false, 0)
			writeBackupPolicyTestResponse(t, writer, http.StatusOK, disabledBackupPolicyJSON())
		default:
			t.Errorf("method = %s, want GET or PUT", request.Method)
			http.Error(writer, "unexpected method", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	_ = executeNoun(
		t,
		newBackupCmd(),
		server.URL,
		Scope{Environment: backupPolicyTestEnvironmentID, AsID: true},
		"policy",
		"set",
		"--off",
	)
}

type backupPolicySetBody struct {
	Enabled     bool    `json:"enabled"`
	Frequency   *string `json:"frequency"`
	Keep        *int64  `json:"keep"`
	Encryption  *string `json:"encryption"`
	ConnectorID *string `json:"connector_id"`
	Sources     []struct {
		Kind     string `json:"kind"`
		TargetID string `json:"target_id"`
	} `json:"sources"`
}

func assertBackupPolicySetRequest(
	t *testing.T,
	request *http.Request,
	enabled bool,
	configured bool,
	expectedKeep int64,
) {
	t.Helper()
	if request.Method != http.MethodPut {
		t.Errorf("method = %s, want PUT", request.Method)
	}
	if request.Header.Get("Idempotency-Key") == "" {
		t.Error("Idempotency-Key is empty")
	}
	if contentType := request.Header.Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", contentType)
	}
	var body backupPolicySetBody
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		t.Fatalf("decode Backup Policy request: %v", err)
	}
	if body.Enabled != enabled {
		t.Errorf("enabled = %t, want %t", body.Enabled, enabled)
	}
	if !configured {
		if body.Frequency != nil || body.Keep != nil || body.Encryption != nil || body.ConnectorID != nil {
			t.Errorf("unconfigured policy contains optional fields: %#v", body)
		}
		if len(body.Sources) != 0 {
			t.Errorf("unconfigured source count = %d, want 0", len(body.Sources))
		}
		return
	}
	if body.Frequency == nil || body.Keep == nil || body.Encryption == nil || body.ConnectorID == nil {
		t.Fatalf("configured policy is missing optional fields: %#v", body)
	}
	if *body.Frequency != "Tue *-*-* 04:30:00" || *body.Keep != expectedKeep || *body.Encryption != "age" {
		t.Errorf("configured policy fields = %#v", body)
	}
	if *body.ConnectorID != backupPolicyConnectorID {
		t.Errorf("connector_id = %q, want %q", *body.ConnectorID, backupPolicyConnectorID)
	}
	want := []struct {
		kind     string
		targetID string
	}{
		{kind: "attach", targetID: backupPolicyAttachID},
		{kind: "volume", targetID: backupPolicyVolumeID},
		{kind: "config", targetID: backupPolicyTestEnvironmentID},
	}
	if len(body.Sources) != len(want) {
		t.Fatalf("source count = %d, want %d", len(body.Sources), len(want))
	}
	for index, source := range body.Sources {
		if source.Kind != want[index].kind || source.TargetID != want[index].targetID {
			t.Errorf("source %d = %#v, want %#v", index, source, want[index])
		}
	}
}

func assertBackupPolicyEnvironmentQuery(t *testing.T, request *http.Request) {
	t.Helper()
	if got := request.URL.Query().Get("environment"); got != backupPolicyTestEnvironmentID {
		t.Errorf("environment = %q, want %q", got, backupPolicyTestEnvironmentID)
	}
}

func writeBackupPolicyTestResponse(t *testing.T, writer io.Writer, status int, body string) {
	t.Helper()
	if responseWriter, ok := writer.(http.ResponseWriter); ok {
		responseWriter.WriteHeader(status)
	}
	if _, err := io.WriteString(writer, body); err != nil {
		t.Errorf("write response: %v", err)
	}
}

func tenantPageJSON() string {
	return fmt.Sprintf(
		`{"items":[{"id":%q,"slug":"acme","name":"Acme","description":""}],"next_cursor":""}`,
		backupPolicyTenantID,
	)
}

func projectPageJSON() string {
	return fmt.Sprintf(
		`{"items":[{`+
			`"id":%q,"tenant_id":%q,"slug":"storefront","name":"Storefront",`+
			`"kind":"tenant","description":""}],"next_cursor":""}`,
		backupPolicyProjectID,
		backupPolicyTenantID,
	)
}

func environmentPageJSON() string {
	return fmt.Sprintf(
		`{"items":[{`+
			`"id":%q,"project_id":%q,"name":"production","network_pool":"10.40.0.0/24",`+
			`"volume_dir":"/var/lib/groundplane/environments/production",`+
			`"provisioning_state":"ready"}],"next_cursor":""}`,
		backupPolicyTestEnvironmentID,
		backupPolicyProjectID,
	)
}

func configuredBackupPolicyJSON(enabled bool) string {
	return fmt.Sprintf(
		`{"enabled":%t,"frequency":"Tue *-*-* 04:30:00","keep":7,`+
			`"encryption":"age","connector_id":%q,"sources":[`+
			`{"id":"spt_01ARZ3NDEKTSV4RRFFQ69G5FAA","kind":"attach","target_id":%q},`+
			`{"id":"spt_01ARZ3NDEKTSV4RRFFQ69G5FAB","kind":"volume","target_id":%q},`+
			`{"id":"spt_01ARZ3NDEKTSV4RRFFQ69G5FAC","kind":"config","target_id":%q}]}`,
		enabled,
		backupPolicyConnectorID,
		backupPolicyAttachID,
		backupPolicyVolumeID,
		backupPolicyTestEnvironmentID,
	)
}

func disabledBackupPolicyJSON() string {
	return `{"enabled":false,"frequency":"","keep":0,"encryption":"","sources":[]}`
}
