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
		{name: "IPv4 literal", value: "127.0.0.1"},
		{name: "IPv6 literal", value: "2001:db8::1"},
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

// Rationale: callers may impose a narrower bound while retaining the shared
// canonical DNS grammar.
func TestValidWithinHonorsBound(t *testing.T) {
	value := strings.Repeat("a.", 110) + "a"
	if len(value) <= 220 || !Valid(value) || ValidWithin(value, 220) {
		t.Fatalf("bounded validation for %q (length %d) did not distinguish 220-byte limit", value, len(value))
	}
}
