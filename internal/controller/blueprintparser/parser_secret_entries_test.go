package blueprintparser

import "testing"

// Rationale: Blueprint input is exportable operator intent, not a secret-value
// transport. A key-only secret Entry is valid; plaintext in that field is not.
func TestParseRequiresEmptySecretEntryLiteral(t *testing.T) {
	for _, candidate := range []struct {
		name    string
		literal string
		valid   bool
	}{
		{name: "empty", literal: "", valid: true},
		{name: "plaintext", literal: "credential", valid: false},
	} {
		t.Run(candidate.name, func(t *testing.T) {
			bundle := parserBundle([]string{"root.yaml"}, map[string]string{
				"root.yaml": environmentRoot(
					"x-gp-entry:\n  API_TOKEN:\n    kind: env\n    source:\n      literal: " + candidate.literal + "\n    exposure: [all]\n    secret: true\nservices: {}\n",
				),
			})
			_, err := Parse(t.Context(), parserEnvironmentScope, bundle)
			if (err == nil) != candidate.valid {
				t.Fatalf("Parse() error = %v, valid = %t", err, candidate.valid)
			}
		})
	}
}
