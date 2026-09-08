package cli

import "testing"

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
