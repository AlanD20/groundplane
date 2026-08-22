package composekey

import "testing"

func TestValidateMatchesComposeResourceKeyGrammar(t *testing.T) {
	// Rationale: direct resource APIs and Blueprint-authored Compose maps must
	// accept and reject the same names rather than creating a second grammar.
	t.Parallel()
	for _, value := range []string{"frontend", "identity-private", "cache.v2", "API_2"} {
		if err := Validate(value); err != nil {
			t.Fatalf("Validate(%q) error = %v", value, err)
		}
	}
	for _, value := range []string{"", "front end", "frontend/net", "cafe\u0301", "network:"} {
		if err := Validate(value); err == nil {
			t.Fatalf("Validate(%q) error = nil", value)
		}
	}
}
