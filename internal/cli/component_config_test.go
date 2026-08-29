package cli

import "testing"

// Rationale: CLI input must not be rewritten into a different value before
// the Controller validates the complete typed configuration.
func TestParseCoreDNSForwardFlagsRejectsWhitespaceInsteadOfNormalizing(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		" example.com=1.1.1.1",
		"example.com =1.1.1.1",
		"example.com= 1.1.1.1",
		"example.com=1.1.1.1, 8.8.8.8",
	} {
		if _, err := parseCoreDNSForwardFlags([]string{value}); err == nil {
			t.Fatalf("parseCoreDNSForwardFlags(%q) accepted input by normalization", value)
		}
	}
	parsed, err := parseCoreDNSForwardFlags([]string{"example.com=1.1.1.1,8.8.8.8"})
	if err != nil {
		t.Fatalf("parseCoreDNSForwardFlags(valid) error = %v", err)
	}
	if len(parsed) != 1 || parsed[0].Domain != "example.com" || len(parsed[0].Resolvers) != 2 || parsed[0].Resolvers[1] != "8.8.8.8" {
		t.Fatalf("parseCoreDNSForwardFlags(valid) = %#v", parsed)
	}
}
