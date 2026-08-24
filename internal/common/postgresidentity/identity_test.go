package postgresidentity

import (
	"strings"
	"testing"
)

// Rationale: typed PostgreSQL execution must accept every identity the
// Controller can generate, including hyphen/underscore service atoms, without
// accepting arbitrary PostgreSQL identifiers or malformed ULID tails.
func TestValidGeneratedMatchesAttachIdentityGrammar(t *testing.T) {
	tests := []struct {
		value string
		valid bool
	}{
		{value: "api-web_tsv4rr", valid: true},
		{value: "api_worker_5d3f9a", valid: true},
		{value: "API.v2_tsv4rr", valid: true},
		{value: "api", valid: false},
		{value: "_tsv4rr", valid: false},
		{value: "api/worker_tsv4rr", valid: false},
		{value: "api_worker_TSV4RR", valid: false},
		{value: "api_worker_iiiiii", valid: false},
		{value: strings.Repeat("a", maximumIdentityBytes-randomTailBytes) + "_tsv4rr", valid: false},
	}
	for _, test := range tests {
		t.Run(test.value, func(t *testing.T) {
			if got := ValidGenerated(test.value); got != test.valid {
				t.Fatalf("ValidGenerated(%q) = %t, want %t", test.value, got, test.valid)
			}
		})
	}
}
