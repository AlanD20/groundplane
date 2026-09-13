package apiclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	generated "github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
)

const validCoreDNSComponentJSON = `{
	"id":"cmp_01K3D7R40G0000000000000000",
	"owner":"platform",
	"kind":"coredns",
	"enabled":true,
	"config":{
		"corefile_template":".:53 {\\n    forward . {groundplane}\\n}",
		"upstream_auto":true,
		"upstream_resolvers":[],
		"forwarders":[],
		"tailnet_delegation":false
	},
	"healthy":true
}`

// Rationale: a top-level nullable union decodes as a zero value in generated
// Go clients; the response envelope must preserve the null config state.
// QA: CMP-01/02, DNS-02; local nullable/closed config decoding only.
func TestGeneratedComponentConfigResponsePreservesNullConfig(t *testing.T) {
	t.Parallel()
	response, err := generated.ParseComponentConfigShowResponse(&http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"config":null,"managed_files":[]}`)),
	})
	if err != nil {
		t.Fatalf("ParseComponentConfigShowResponse() error = %v", err)
	}
	if response.JSON200 == nil {
		t.Fatal("ParseComponentConfigShowResponse() returned nil response")
	}
	if response.JSON200.Config != nil {
		t.Fatalf("generated response config = %#v, want nil", response.JSON200.Config)
	}
	if response.JSON200.ManagedFiles == nil || len(response.JSON200.ManagedFiles) != 0 {
		t.Fatalf("generated response managed_files = %#v, want non-null empty array", response.JSON200.ManagedFiles)
	}
}

// Rationale: platform CoreDNS is a valid closed Component config variant even
// though permissive generated oneOf branches overlap; CLI list and show reads
// must reach the strict public API decoder instead of failing in eager parsing.
// QA: CMP-01/02, DNS-02; local nullable/closed config decoding only.
func TestComponentClientDecodesValidPlatformCoreDNSListAndShow(t *testing.T) {
	t.Parallel()
	const componentID = "cmp_01K3D7R40G0000000000000000"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/components":
			if request.Method != http.MethodGet || request.URL.Query().Get("platform") != "true" {
				t.Errorf("list method/query = %s/%q", request.Method, request.URL.RawQuery)
			}
			if _, err := io.WriteString(w, `{"items":[`+validCoreDNSComponentJSON+`]}`); err != nil {
				t.Errorf("write list response: %v", err)
			}
		case "/api/v1/components/" + componentID:
			if request.Method != http.MethodGet {
				t.Errorf("show method = %s", request.Method)
			}
			if _, err := io.WriteString(w, validCoreDNSComponentJSON); err != nil {
				t.Errorf("write show response: %v", err)
			}
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	client := New(server.URL)
	page, err := client.ListComponents(context.Background(), "", true, "", 0, "")
	if err != nil || len(page.Items) != 1 || page.Items[0].Config == nil || page.Items[0].Config.CoreDNS == nil {
		t.Fatalf("ListComponents() = %#v, %v", page, err)
	}
	shown, err := client.ShowComponent(context.Background(), componentID)
	if err != nil || shown.Config == nil || shown.Config.CoreDNS == nil || requests != 2 {
		t.Fatalf("ShowComponent() = %#v, %v; requests = %d", shown, err, requests)
	}
}

// Rationale: bypassing the generated eager oneOf parser must not make the CLI
// accept incomplete Component variants; malformed responses still fail through
// the strict public API decoder.
// QA: CMP-01/02, DNS-02; local nullable/closed config decoding only.
func TestComponentClientRejectsMalformedCoreDNSListResponse(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, err := io.WriteString(w, `{"items":[{
			"id":"cmp_01K3D7R40G0000000000000000",
			"owner":"platform",
			"kind":"coredns",
			"enabled":true,
			"config":{
				"upstream_auto":true,
				"upstream_resolvers":[],
				"forwarders":[],
				"tailnet_delegation":false
			},
			"healthy":true
		}]}`)
		if err != nil {
			t.Errorf("write malformed list response: %v", err)
		}
	}))
	defer server.Close()

	page, err := New(server.URL).ListComponents(context.Background(), "", true, "", 0, "")
	if err == nil {
		t.Fatalf("ListComponents() = %#v, nil error; want malformed response rejection", page)
	}
}
