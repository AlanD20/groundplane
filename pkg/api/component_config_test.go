package api

import (
	"encoding/json"
	"strings"
	"testing"
)

// QA: CMP-01, DNS-02, UI-03 - L0 response-model JSON only; no persisted config,
// registered planner, managed-file render, or Component activation is exercised.
// Rationale: the wire contract must not silently accept a second variant or
// turn an absent/invalid configuration into a permissive empty object.
func TestComponentConfigIsAClosedExactlyOneWireUnion(t *testing.T) {
	t.Parallel()
	valid := []struct {
		name string
		json string
		want string
	}{
		{
			name: "caddy",
			json: `{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX"]}`,
			want: `{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX"]}`,
		},
		{
			name: "cloudflare",
			json: `{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX"],"secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`,
			want: `{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX"],"secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`,
		},
		{
			name: "coredns",
			json: `{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[],"forwarders":[],"tailnet_delegation":false}`,
			want: `{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[],"forwarders":[],"tailnet_delegation":false}`,
		},
		{
			name: "coredns-forwarder",
			json: `{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":false,"upstream_resolvers":["1.1.1.1"],"forwarders":[{"domain":"example.com","resolvers":["9.9.9.9"]}],"tailnet_delegation":true}`,
			want: `{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":false,"upstream_resolvers":["1.1.1.1"],"forwarders":[{"domain":"example.com","resolvers":["9.9.9.9"]}],"tailnet_delegation":true}`,
		},
	}
	for _, testCase := range valid {
		t.Run(testCase.name, func(t *testing.T) {
			var config ComponentConfig
			if err := json.Unmarshal([]byte(testCase.json), &config); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			encoded, err := json.Marshal(config)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			if string(encoded) != testCase.want {
				t.Fatalf("Marshal() = %s, want %s", encoded, testCase.want)
			}
		})
	}
	for _, value := range []string{
		`{}`,
		`{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX"],"caddyfile_template":"{gp.routes}","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`,
		`{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX"],"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[],"forwarders":[],"tailnet_delegation":false}`,
		`{"unknown":true}`,
	} {
		var config ComponentConfig
		if err := json.Unmarshal([]byte(value), &config); err == nil {
			t.Fatalf("Unmarshal(%s) accepted an empty, mixed, or unknown variant", value)
		}
	}
}

// QA: CMP-01, DNS-02, UI-03 - L0 mutation-model JSON only; no handler validation,
// desired-state publication, rendering, or runtime reconciliation is exercised.
// Rationale: mutation requests are the trust boundary, so null and
// incomplete fields must not be normalized into a different variant.
func TestComponentConfigMutationInputRejectsEmptyAndMixedVariants(t *testing.T) {
	t.Parallel()
	valid := ComponentConfigMutationInput{
		Caddy: &CaddyComponentConfigMutationInput{ZoneIDs: []string{"net_01ARZ3NDEKTSV4RRFFQ69G5FAX"}},
	}
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatalf("Marshal(valid) error = %v", err)
	}
	if string(encoded) != `{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX"]}` {
		t.Fatalf("Marshal(valid) = %s", encoded)
	}
	for _, value := range []string{
		`{}`,
		`{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX"],"caddyfile_template":"{gp.routes}","credential":{"mode":"existing","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}}`,
		`{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[]}`,
		`{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[null],"forwarders":[],"tailnet_delegation":false}`,
		`{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[],"forwarders":[{}],"tailnet_delegation":false}`,
		`{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[],"forwarders":[{"domain":"example.com","resolvers":[null]}],"tailnet_delegation":false}`,
		`{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[],"forwarders":[{"domain":"example.com","resolvers":[],"unknown":true}],"tailnet_delegation":false}`,
	} {
		var input ComponentConfigMutationInput
		if err := json.Unmarshal([]byte(value), &input); err == nil {
			t.Fatalf("Unmarshal(%s) accepted an empty, mixed, or incomplete variant", value)
		}
	}
	if _, err := json.Marshal(ComponentConfigMutationInput{}); err == nil ||
		!strings.Contains(err.Error(), "exactly one variant") {
		t.Fatalf("Marshal(empty) error = %v, want exactly-one validation", err)
	}
}

// QA: CMP-01, CMP-02, DNS-02, UI-03 - L0 public-model validation only; no
// Controller read revision, renderer output, persistence, or serving DNS is proved.
// Rationale: presence-aware decoding and response validation prevent invalid
// persisted state from crossing the public API boundary.
func TestComponentConfigRejectsExplicitNullAndInvalidResponseState(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		`{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX"],"secret_id":null}`,
		`{"zone_ids":["net_01ARZ3NDEKTSV4RRFFQ69G5FAX"],"caddyfile_template":null}`,
		`{"secret_id":"sec_invalid"}`,
		`{"corefile_template":null,"upstream_auto":true,"upstream_resolvers":[],"forwarders":[],"tailnet_delegation":false}`,
		`{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":null,"forwarders":[],"tailnet_delegation":false}`,
		`{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[null],"forwarders":[],"tailnet_delegation":false}`,
		`{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[],"forwarders":[null],"tailnet_delegation":false}`,
		`{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[],"forwarders":[{"domain":"example.com"}],"tailnet_delegation":false}`,
		`{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[],"forwarders":[{"domain":"example.com","resolvers":[],"unknown":null}],"tailnet_delegation":false}`,
		`{"corefile_template":". {\n    {groundplane}\n}\n","upstream_auto":true,"upstream_resolvers":[],"forwarders":[{"domain":"example.com","resolvers":null}],"tailnet_delegation":false}`,
	} {
		var config ComponentConfig
		if err := json.Unmarshal([]byte(value), &config); err == nil {
			t.Fatalf("Unmarshal(%s) accepted invalid response state", value)
		}
	}
	if _, err := json.Marshal(ComponentConfig{Caddy: &CaddyComponentConfig{ZoneIDs: []string{"net_invalid"}}}); err == nil {
		t.Fatal("Marshal accepted an invalid Caddy zone_id")
	}
	if _, err := json.Marshal(ComponentConfig{CoreDNS: &CoreDNSComponentConfig{
		CorefileTemplate:  ". {\n    {groundplane}\n}\n",
		UpstreamResolvers: nil, Forwarders: []ComponentDNSForwarder{},
	}}); err == nil {
		t.Fatal("Marshal accepted nil CoreDNS arrays")
	}
}

// QA: CMP-02, UI-01 - L0 response-envelope encoding only; no Component GET,
// fixed-revision read, managed-file content, or live serving bytes are exercised.
// Rationale: managed configuration previews are a generic read projection, so
// every Component config response must preserve an array even when no provider
// supplies a preview.
func TestComponentConfigResponseAlwaysIncludesManagedFilesArray(t *testing.T) {
	t.Parallel()
	encoded, err := json.Marshal(ComponentConfigResponse{
		Config:       nil,
		ManagedFiles: []ManagedConfigFile{},
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if string(encoded) != `{"config":null,"managed_files":[]}` {
		t.Fatalf("Marshal() = %s, want non-null managed_files", encoded)
	}
}

// QA: CMP-01, HTTP-08, UI-03 - L0 credential decoding only; no Secret ownership,
// token storage/redaction, Tunnel publication, or provider ingress is exercised.
// Rationale: existing and new credentials have different secret ownership
// semantics; accepting unknown or mixed modes could expose write-only data.
func TestCloudflareTunnelCredentialRequiresKnownExclusiveMode(t *testing.T) {
	t.Parallel()
	valid := []struct {
		value string
		want  CloudflareTunnelCredentialInput
	}{
		{
			value: `{"mode":"existing","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`,
			want: CloudflareTunnelCredentialInput{
				Mode: "existing", SecretID: "sec_01ARZ3NDEKTSV4RRFFQ69G5FAX",
			},
		},
		{
			value: `{"mode":"new","secret_name":"TUNNEL_TOKEN","token":"token"}`,
			want: CloudflareTunnelCredentialInput{
				Mode: "new", SecretName: "TUNNEL_TOKEN", Token: "token",
			},
		},
	}
	for _, test := range valid {
		var credential CloudflareTunnelCredentialInput
		if err := json.Unmarshal([]byte(test.value), &credential); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", test.value, err)
		}
		if credential != test.want {
			t.Fatalf("Unmarshal(%s) = %#v, want %#v", test.value, credential, test.want)
		}
	}
	for _, value := range []string{
		`{"mode":"unknown","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`,
		`{"mode":"existing","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX","token":null}`,
		`{"mode":"existing","secret_id":null}`,
		`{"mode":"new","secret_name":"TUNNEL_TOKEN","token":"x","secret_id":null}`,
		`{"mode":"new","secret_name":"TUNNEL_TOKEN","token":""}`,
		`{"mode":"new","secret_name":"TUNNEL_TOKEN"}`,
	} {
		var credential CloudflareTunnelCredentialInput
		if err := json.Unmarshal([]byte(value), &credential); err == nil {
			t.Fatalf("Unmarshal(%s) accepted invalid credential", value)
		}
	}
}
