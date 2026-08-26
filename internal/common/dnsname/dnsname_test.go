package dnsname

import (
	"strings"
	"testing"
)

// Rationale: Route and CoreDNS must share lowercase ASCII label rules while
// CoreDNS forwarder domains additionally enforce their DNS wire-size bound.
func TestValidCanonicalDNSName(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{name: "single label", value: "example", valid: true},
		{name: "dotted name", value: "api.example.com", valid: true},
		{name: "uppercase", value: "API.example.com"},
		{name: "wildcard", value: "*.example.com"},
		{name: "trailing dot", value: "example.com."},
		{name: "empty label", value: "api..example.com"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Valid(test.value); got != test.valid {
				t.Fatalf("Valid(%q) = %t, want %t", test.value, got, test.valid)
			}
		})
	}
}

// Rationale: the forwarder domain bound is narrower than a Route hostname,
// but must use the same canonical label grammar.
func TestValidWithinHonorsBound(t *testing.T) {
	value := strings.Repeat("a.", 108) + "aaa"
	if len(value) <= 218 || !Valid(value) || ValidWithin(value, 218) {
		t.Fatalf("bounded validation for %q (length %d) did not distinguish 218-byte limit", value, len(value))
	}
}
