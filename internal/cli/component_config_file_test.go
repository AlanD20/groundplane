package cli

import "testing"

// QA: CMP-01, HTTP-08, UI-03; pure file decoding only, not Secret resolution or Tunnel activation.
// Rationale: Tunnel files must reject unknown fields and trailing documents so
// mixed or ambiguous configuration cannot reach Zone placement or mutation.
func TestDecodeCloudflareTunnelConfigFileIsClosedBeforePlacement(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{
			name:  "unknown outer field",
			value: `{"credential":{"mode":"new","secret_name":"TOKEN","token":"value"},"caddyfile_template":"mixed"}`,
		},
		{
			name:  "unknown credential field",
			value: `{"credential":{"mode":"new","secret_name":"TOKEN","token":"value","secret_id":"sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"}}`,
		},
		{
			name:  "multiple documents",
			value: `{"credential":{"mode":"new","secret_name":"TOKEN","token":"value"}} {}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeCloudflareTunnelConfigFile(test.value, true); err == nil {
				t.Fatalf("decodeCloudflareTunnelConfigFile(%s) error = nil", test.name)
			}
		})
	}
}

// QA: CMP-01, HTTP-08, UI-03; pure file decoding only, not the later Zone lookup or API request.
// Rationale: a credential-only Tunnel file is complete only when the command
// supplies placement separately; otherwise it must not invent an empty Zone set.
func TestDecodeCloudflareTunnelConfigFileAllowsMissingZonesOnlyForExplicitPlacement(t *testing.T) {
	value := `{"credential":{"mode":"new","secret_name":"TOKEN","token":"value"}}`
	if _, err := decodeCloudflareTunnelConfigFile(value, false); err == nil {
		t.Fatal("credential-only file without explicit placement was accepted")
	}
	input, err := decodeCloudflareTunnelConfigFile(value, true)
	if err != nil {
		t.Fatalf("credential-only file with explicit placement: %v", err)
	}
	if input.CloudflareTunnel == nil || input.CloudflareTunnel.Credential.SecretName != "TOKEN" ||
		len(input.CloudflareTunnel.ZoneIDs) != 0 {
		t.Fatalf("decoded input = %#v", input)
	}
}
