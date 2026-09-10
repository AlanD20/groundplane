package cli

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/cli/apiclient"
	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	"github.com/spf13/cobra"
)

const (
	componentTestID    = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	environmentTestID  = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	frontendZoneID     = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	identityZoneID     = "net_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	createdEdgeZoneID  = "net_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	createdInnerZoneID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAY"
)

func TestComponentEnableResolvesAndCreatesOrderedZones(t *testing.T) {
	template := "{\n\t{gp.routes}\n}\n"
	path := t.TempDir() + "/Caddyfile"
	if err := os.WriteFile(path, []byte(template), 0o600); err != nil {
		t.Fatal(err)
	}

	requestNumber := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNumber++
		writer.Header().Set("Content-Type", "application/json")
		switch requestNumber {
		case 1:
			assertComponentRequest(t, request, http.MethodGet, "/api/v1/components/"+componentTestID, "")
			_, _ = io.WriteString(
				writer,
				`{"id":"`+componentTestID+`","owner":"environment","owner_id":"`+environmentTestID+`","environment_id":"`+environmentTestID+`","kind":"caddy","enabled":false,"config":null,"healthy":false,"status":"disabled"}`,
			)
		case 2:
			assertComponentRequest(t, request, http.MethodGet, "/api/v1/zones", "")
			if got := request.URL.Query().Get("environment"); got != environmentTestID {
				t.Errorf("environment query = %q, want %q", got, environmentTestID)
			}
			_, _ = io.WriteString(
				writer,
				`{"items":[{"id":"`+frontendZoneID+`","environment_id":"`+environmentTestID+`","name":"frontend","subnet":"10.40.10.0/24","internal":false,"owner_kind":"environment","owner_id":"`+environmentTestID+`"},{"id":"`+identityZoneID+`","environment_id":"`+environmentTestID+`","name":"identity-private","subnet":"10.40.20.0/24","internal":true,"owner_kind":"environment","owner_id":"`+environmentTestID+`"}],"next_cursor":""}`,
			)
		case 3:
			want := `{"environment_id":"` + environmentTestID + `","internal":false,"name":"edge","subnet":"10.40.30.0/24"}`
			assertComponentRequest(t, request, http.MethodPost, "/api/v1/zones", want)
			writer.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(
				writer,
				`{"id":"`+createdEdgeZoneID+`","environment_id":"`+environmentTestID+`","name":"edge","subnet":"10.40.30.0/24","internal":false,"owner_kind":"environment","owner_id":"`+environmentTestID+`"}`,
			)
		case 4:
			want := `{"environment_id":"` + environmentTestID + `","internal":true,"name":"private","subnet":"10.40.40.0/24"}`
			assertComponentRequest(t, request, http.MethodPost, "/api/v1/zones", want)
			writer.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(
				writer,
				`{"id":"`+createdInnerZoneID+`","environment_id":"`+environmentTestID+`","name":"private","subnet":"10.40.40.0/24","internal":true,"owner_kind":"environment","owner_id":"`+environmentTestID+`"}`,
			)
		case 5:
			want := `{"config":{"zone_ids":["` + frontendZoneID + `","` + identityZoneID + `","` + createdEdgeZoneID + `","` + createdInnerZoneID + `"],"caddyfile_template":"{\n\t{gp.routes}\n}\n"}}`
			assertComponentRequest(t, request, http.MethodPost, "/api/v1/components/"+componentTestID+"/enable", want)
			writer.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(writer, `{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`)
		default:
			t.Fatalf("unexpected request %d: %s %s", requestNumber, request.Method, request.URL.RequestURI())
		}
	}))
	defer server.Close()

	_, err := executeComponentForTest(t, server.URL, Scope{},
		"enable", componentTestID,
		"--zone", "frontend", "--zone", "identity-private",
		"--create-zone", "edge=10.40.30.0/24",
		"--create-internal-zone", "private=10.40.40.0/24",
		"--file", path,
	)
	if err != nil {
		t.Fatalf("execute enable: %v", err)
	}
	if requestNumber != 5 {
		t.Fatalf("requests = %d, want 5", requestNumber)
	}
}

func TestComponentEnableReportsCreatedZoneRetainedAfterFailure(t *testing.T) {
	requestNumber := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNumber++
		writer.Header().Set("Content-Type", "application/json")
		switch requestNumber {
		case 1:
			_, _ = io.WriteString(
				writer,
				`{"id":"`+componentTestID+`","owner":"environment","owner_id":"`+environmentTestID+`","environment_id":"`+environmentTestID+`","kind":"caddy","enabled":false,"config":null,"healthy":false,"status":"disabled"}`,
			)
		case 2:
			writer.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(
				writer,
				`{"id":"`+createdEdgeZoneID+`","environment_id":"`+environmentTestID+`","name":"edge","subnet":"10.40.30.0/24","internal":false,"owner_kind":"environment","owner_id":"`+environmentTestID+`"}`,
			)
		case 3:
			writer.Header().Set("Content-Type", "application/problem+json")
			writer.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = io.WriteString(
				writer,
				`{"type":"about:blank","title":"Validation failed","status":422,"detail":"enable rejected","code":"validation.failed"}`,
			)
		default:
			t.Fatalf("unexpected request %d", requestNumber)
		}
	}))
	defer server.Close()

	output, err := executeComponentForTest(t, server.URL, Scope{},
		"enable", componentTestID, "--create-zone", "edge=10.40.30.0/24",
	)
	if err == nil {
		t.Fatal("execute enable error = nil")
	}
	if !strings.Contains(output, "edge") || !strings.Contains(output, createdEdgeZoneID) ||
		!strings.Contains(output, "remains") {
		t.Fatalf("warning output = %q", output)
	}
	if requestNumber != 3 {
		t.Fatalf("requests = %d, want show, create, enable only", requestNumber)
	}
}

func TestComponentTunnelFirstEnableCombinesCredentialFileWithCreatedZone(t *testing.T) {
	path := t.TempDir() + "/tunnel.json"
	if err := os.WriteFile(
		path,
		[]byte(`{"credential":{"mode":"new","secret_name":"TUNNEL_TOKEN","token":"private-token"}}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	requestNumber := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNumber++
		writer.Header().Set("Content-Type", "application/json")
		switch requestNumber {
		case 1:
			assertComponentRequest(t, request, http.MethodGet, "/api/v1/components/"+componentTestID, "")
			_, _ = io.WriteString(
				writer,
				`{"id":"`+componentTestID+`","owner":"environment","owner_id":"`+environmentTestID+`","environment_id":"`+environmentTestID+`","kind":"cloudflare-tunnel","enabled":false,"config":null,"healthy":false,"status":"disabled"}`,
			)
		case 2:
			want := `{"environment_id":"` + environmentTestID + `","internal":false,"name":"frontend","subnet":"10.40.10.0/24"}`
			assertComponentRequest(t, request, http.MethodPost, "/api/v1/zones", want)
			writer.WriteHeader(http.StatusCreated)
			_, _ = io.WriteString(
				writer,
				`{"id":"`+frontendZoneID+`","environment_id":"`+environmentTestID+`","name":"frontend","subnet":"10.40.10.0/24","internal":false,"owner_kind":"environment","owner_id":"`+environmentTestID+`"}`,
			)
		case 3:
			want := `{"config":{"zone_ids":["` + frontendZoneID + `"],"credential":{"mode":"new","secret_name":"TUNNEL_TOKEN","token":"private-token"}}}`
			assertComponentRequest(t, request, http.MethodPost, "/api/v1/components/"+componentTestID+"/enable", want)
			writer.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(writer, `{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`)
		default:
			t.Fatalf("unexpected request %d", requestNumber)
		}
	}))
	defer server.Close()

	_, err := executeComponentForTest(t, server.URL, Scope{},
		"enable", componentTestID,
		"--file", path,
		"--create-zone", "frontend=10.40.10.0/24",
	)
	if err != nil {
		t.Fatalf("execute first Tunnel enable: %v", err)
	}
	if requestNumber != 3 {
		t.Fatalf("requests = %d, want show, create, enable", requestNumber)
	}
}

func TestComponentTunnelFirstEnableCombinesCredentialFileWithExistingZone(t *testing.T) {
	path := t.TempDir() + "/tunnel.json"
	if err := os.WriteFile(
		path,
		[]byte(`{"credential":{"mode":"new","secret_name":"TUNNEL_TOKEN","token":"private-token"}}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	requestNumber := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNumber++
		writer.Header().Set("Content-Type", "application/json")
		switch requestNumber {
		case 1:
			_, _ = io.WriteString(
				writer,
				`{"id":"`+componentTestID+`","owner":"environment","owner_id":"`+environmentTestID+`","environment_id":"`+environmentTestID+`","kind":"cloudflare-tunnel","enabled":false,"config":null,"healthy":false,"status":"disabled"}`,
			)
		case 2:
			_, _ = io.WriteString(
				writer,
				`{"items":[{"id":"`+frontendZoneID+`","environment_id":"`+environmentTestID+`","name":"frontend","subnet":"10.40.10.0/24","internal":false,"owner_kind":"environment","owner_id":"`+environmentTestID+`"}],"next_cursor":""}`,
			)
		case 3:
			want := `{"config":{"zone_ids":["` + frontendZoneID + `"],"credential":{"mode":"new","secret_name":"TUNNEL_TOKEN","token":"private-token"}}}`
			assertComponentRequest(t, request, http.MethodPost, "/api/v1/components/"+componentTestID+"/enable", want)
			writer.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(writer, `{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`)
		default:
			t.Fatalf("unexpected request %d", requestNumber)
		}
	}))
	defer server.Close()

	_, err := executeComponentForTest(t, server.URL, Scope{},
		"enable", componentTestID, "--file", path, "--zone", "frontend",
	)
	if err != nil {
		t.Fatalf("execute first Tunnel enable: %v", err)
	}
	if requestNumber != 3 {
		t.Fatalf("requests = %d, want show, resolve, enable", requestNumber)
	}
}

func TestComponentTunnelInvalidCredentialStopsBeforeZoneCreation(t *testing.T) {
	path := t.TempDir() + "/tunnel.json"
	if err := os.WriteFile(
		path,
		[]byte(`{"credential":{"mode":"new","secret_name":"TUNNEL_TOKEN"}}`),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if requests != 1 {
			t.Fatalf("unexpected mutation after invalid credential: %s %s", request.Method, request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(
			writer,
			`{"id":"`+componentTestID+`","owner":"environment","owner_id":"`+environmentTestID+`","environment_id":"`+environmentTestID+`","kind":"cloudflare-tunnel","enabled":false,"config":null,"healthy":false,"status":"disabled"}`,
		)
	}))
	defer server.Close()

	_, err := executeComponentForTest(t, server.URL, Scope{},
		"enable", componentTestID,
		"--file", path,
		"--create-zone", "frontend=10.40.10.0/24",
	)
	if err == nil {
		t.Fatal("execute first Tunnel enable error = nil")
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want show only", requests)
	}
}

func TestPlatformComponentEnableRejectsInlineConfigBeforeMutation(t *testing.T) {
	path := t.TempDir() + "/Corefile"
	if err := os.WriteFile(path, []byte(".:53 {\n  forward . 1.1.1.1\n}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if requests != 1 {
			t.Fatalf("unexpected platform enable mutation: %s %s", request.Method, request.URL.Path)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(
			writer,
			`{"id":"`+componentTestID+`","owner":"platform","owner_id":"platform","environment_id":"","kind":"coredns","enabled":false,"config":null,"healthy":false,"status":"disabled"}`,
		)
	}))
	defer server.Close()

	_, err := executeComponentForTest(t, server.URL, Scope{},
		"enable", componentTestID, "--template-file", path,
	)
	if err == nil || !strings.Contains(err.Error(), "config set") || !strings.Contains(err.Error(), "bodyless enable") {
		t.Fatalf("error = %v, want config-set then bodyless-enable guidance", err)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want show only", requests)
	}
}

func TestComponentTunnelConfigResolvesZonesAndPreservesExistingCredential(t *testing.T) {
	const secretID = "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	requestNumber := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNumber++
		writer.Header().Set("Content-Type", "application/json")
		switch requestNumber {
		case 1:
			_, _ = io.WriteString(
				writer,
				`{"id":"`+componentTestID+`","owner":"environment","owner_id":"`+environmentTestID+`","environment_id":"`+environmentTestID+`","kind":"cloudflare-tunnel","enabled":true,"config":null,"healthy":true,"status":"healthy"}`,
			)
		case 2:
			_, _ = io.WriteString(
				writer,
				`{"items":[{"id":"`+frontendZoneID+`","environment_id":"`+environmentTestID+`","name":"frontend","subnet":"10.40.10.0/24","internal":false,"owner_kind":"environment","owner_id":"`+environmentTestID+`"},{"id":"`+identityZoneID+`","environment_id":"`+environmentTestID+`","name":"identity-private","subnet":"10.40.20.0/24","internal":true,"owner_kind":"environment","owner_id":"`+environmentTestID+`"}],"next_cursor":""}`,
			)
		case 3:
			_, _ = io.WriteString(
				writer,
				`{"config":{"zone_ids":["`+createdEdgeZoneID+`"],"secret_id":"`+secretID+`"},"managed_files":[]}`,
			)
		case 4:
			want := `{"config":{"zone_ids":["` + frontendZoneID + `","` + identityZoneID + `"],"credential":{"mode":"existing","secret_id":"` + secretID + `"}}}`
			assertComponentRequest(t, request, http.MethodPut, "/api/v1/components/"+componentTestID+"/config", want)
			_, _ = io.WriteString(
				writer,
				`{"resource":{"zone_ids":["`+frontendZoneID+`","`+identityZoneID+`"],"secret_id":"`+secretID+`"},"reconcile_task_id":null}`,
			)
		default:
			t.Fatalf("unexpected request %d", requestNumber)
		}
	}))
	defer server.Close()

	_, err := executeComponentForTest(t, server.URL, Scope{},
		"config", "set", componentTestID,
		"--zone", "frontend", "--zone", "identity-private",
	)
	if err != nil {
		t.Fatalf("execute config set: %v", err)
	}
}

func TestComponentTunnelFirstZoneConfigRequiresFile(t *testing.T) {
	requestNumber := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestNumber++
		writer.Header().Set("Content-Type", "application/json")
		switch requestNumber {
		case 1:
			_, _ = io.WriteString(
				writer,
				`{"id":"`+componentTestID+`","owner":"environment","owner_id":"`+environmentTestID+`","environment_id":"`+environmentTestID+`","kind":"cloudflare-tunnel","enabled":false,"config":null,"healthy":false,"status":"disabled"}`,
			)
		case 2:
			_, _ = io.WriteString(
				writer,
				`{"items":[{"id":"`+frontendZoneID+`","environment_id":"`+environmentTestID+`","name":"frontend","subnet":"10.40.10.0/24","internal":false,"owner_kind":"environment","owner_id":"`+environmentTestID+`"}],"next_cursor":""}`,
			)
		default:
			t.Fatalf("unexpected mutation request %d", requestNumber)
		}
	}))
	defer server.Close()

	_, err := executeComponentForTest(t, server.URL, Scope{},
		"config", "set", componentTestID, "--zone", "frontend",
	)
	if err == nil || !strings.Contains(err.Error(), "requires --file") {
		t.Fatalf("error = %v, want requires --file", err)
	}
	if requestNumber != 2 {
		t.Fatalf("requests = %d, want no mutation", requestNumber)
	}
}

func TestComponentConfigHasNoSupersededZoneIDFlag(t *testing.T) {
	config, _, err := newComponentCmd().Find([]string{"config", "set"})
	if err != nil {
		t.Fatal(err)
	}
	if config.Flags().Lookup("zone-id") != nil {
		t.Fatal("config set still exposes superseded --zone-id")
	}
}

func TestResolveComponentZoneIDsUsesRawStableIDsWithGlobalID(t *testing.T) {
	command := &cobra.Command{}
	command.SetContext(context.WithValue(context.Background(), appKey{}, &App{
		Client: apiclient.New("http://invalid"), Scope: Scope{AsID: true},
	}))
	want := []string{frontendZoneID, identityZoneID}
	got, err := resolveComponentZoneIDs(command, environmentTestID, want)
	if err != nil {
		t.Fatalf("resolveComponentZoneIDs() error = %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("resolved ids = %q, want %q", got, want)
	}
}

func assertComponentRequest(t *testing.T, request *http.Request, method, path, body string) {
	t.Helper()
	if request.Method != method || request.URL.Path != path {
		t.Errorf("request = %s %s, want %s %s", request.Method, request.URL.Path, method, path)
	}
	got, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Errorf("body = %s, want %s", got, body)
	}
}

func executeComponentForTest(t *testing.T, baseURL string, scope Scope, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	command := newComponentCmd()
	command.SetContext(context.WithValue(context.Background(), appKey{}, &App{
		Client: apiclient.New(baseURL), Out: clicommon.NewWriter(clicommon.FormatJSON, true, &output), Scope: scope,
	}))
	command.SetOut(&output)
	command.SetErr(&output)
	command.SetArgs(args)
	err := command.Execute()
	return output.String(), err
}
