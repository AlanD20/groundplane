package api

import (
	"encoding/json"
	"strings"
	"testing"
)

// Rationale: the wire contract must not silently accept a second variant or
// turn an absent/invalid configuration into a permissive empty object.
func TestComponentConfigIsAClosedExactlyOneWireUnion(t *testing.T) {
	t.Parallel()
	valid := []struct {
		name string
		json string
		want string
	}{
		{name: "caddy", json: `{"zone_id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`, want: `{"zone_id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`},
		{name: "cloudflare", json: `{"secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`, want: `{"secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`},
		{name: "coredns", json: `{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[],"tailnet_delegation":false}`, want: `{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[],"tailnet_delegation":false}`},
		{name: "coredns-forwarder", json: `{"upstream_auto":false,"upstream_resolvers":["1.1.1.1"],"forwarders":[{"domain":"example.com","resolvers":["9.9.9.9"]}],"tailnet_delegation":true}`, want: `{"upstream_auto":false,"upstream_resolvers":["1.1.1.1"],"forwarders":[{"domain":"example.com","resolvers":["9.9.9.9"]}],"tailnet_delegation":true}`},
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
		`{"zone_id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAX","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`,
		`{"zone_id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAX","upstream_auto":true,"upstream_resolvers":[],"forwarders":[],"tailnet_delegation":false}`,
		`{"unknown":true}`,
	} {
		var config ComponentConfig
		if err := json.Unmarshal([]byte(value), &config); err == nil {
			t.Fatalf("Unmarshal(%s) accepted an empty, mixed, or unknown variant", value)
		}
	}
}

// Rationale: mutation requests are the trust boundary, so null and
// incomplete fields must not be normalized into a different variant.
func TestComponentConfigMutationInputRejectsEmptyAndMixedVariants(t *testing.T) {
	t.Parallel()
	valid := ComponentConfigMutationInput{
		Caddy: &CaddyComponentConfigMutationInput{ZoneID: "net_01ARZ3NDEKTSV4RRFFQ69G5FAX"},
	}
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatalf("Marshal(valid) error = %v", err)
	}
	if string(encoded) != `{"zone_id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAX"}` {
		t.Fatalf("Marshal(valid) = %s", encoded)
	}
	for _, value := range []string{
		`{}`,
		`{"zone_id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAX","credential":{"mode":"existing","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}}`,
		`{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[]}`,
		`{"upstream_auto":true,"upstream_resolvers":[null],"forwarders":[],"tailnet_delegation":false}`,
		`{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[{}],"tailnet_delegation":false}`,
		`{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[{"domain":"example.com","resolvers":[null]}],"tailnet_delegation":false}`,
		`{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[{"domain":"example.com","resolvers":[],"unknown":true}],"tailnet_delegation":false}`,
	} {
		var input ComponentConfigMutationInput
		if err := json.Unmarshal([]byte(value), &input); err == nil {
			t.Fatalf("Unmarshal(%s) accepted an empty, mixed, or incomplete variant", value)
		}
	}
	if _, err := json.Marshal(ComponentConfigMutationInput{}); err == nil || !strings.Contains(err.Error(), "exactly one variant") {
		t.Fatalf("Marshal(empty) error = %v, want exactly-one validation", err)
	}
}

// Rationale: presence-aware decoding and response validation prevent invalid
// persisted state from crossing the public API boundary.
func TestComponentConfigRejectsExplicitNullAndInvalidResponseState(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		`{"zone_id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAX","secret_id":null}`,
		`{"zone_id":"net_01ARZ3NDEKTSV4RRFFQ69G5FAX","caddyfile_template":null}`,
		`{"secret_id":"sec_invalid"}`,
		`{"upstream_auto":true,"upstream_resolvers":null,"forwarders":[],"tailnet_delegation":false}`,
		`{"upstream_auto":true,"upstream_resolvers":[null],"forwarders":[],"tailnet_delegation":false}`,
		`{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[null],"tailnet_delegation":false}`,
		`{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[{"domain":"example.com"}],"tailnet_delegation":false}`,
		`{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[{"domain":"example.com","resolvers":[],"unknown":null}],"tailnet_delegation":false}`,
		`{"upstream_auto":true,"upstream_resolvers":[],"forwarders":[{"domain":"example.com","resolvers":null}],"tailnet_delegation":false}`,
	} {
		var config ComponentConfig
		if err := json.Unmarshal([]byte(value), &config); err == nil {
			t.Fatalf("Unmarshal(%s) accepted invalid response state", value)
		}
	}
	if _, err := json.Marshal(ComponentConfig{Caddy: &CaddyComponentConfig{ZoneID: "net_invalid"}}); err == nil {
		t.Fatal("Marshal accepted an invalid Caddy zone_id")
	}
	if _, err := json.Marshal(ComponentConfig{CoreDNS: &CoreDNSComponentConfig{
		UpstreamResolvers: nil, Forwarders: []ComponentDNSForwarder{},
	}}); err == nil {
		t.Fatal("Marshal accepted nil CoreDNS arrays")
	}
}

// Rationale: existing and new credentials have different secret ownership
// semantics; accepting unknown or mixed modes could expose write-only data.
func TestCloudflareTunnelCredentialRequiresKnownExclusiveMode(t *testing.T) {
	t.Parallel()
	valid := []string{
		`{"mode":"existing","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAX"}`,
		`{"mode":"new","secret_name":"TUNNEL_TOKEN","token":"token"}`,
	}
	for _, value := range valid {
		var credential CloudflareTunnelCredentialInput
		if err := json.Unmarshal([]byte(value), &credential); err != nil {
			t.Fatalf("Unmarshal(%s) error = %v", value, err)
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
