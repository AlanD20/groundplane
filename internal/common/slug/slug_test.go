package slug

import (
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: slug callers must share one strict lowercase ASCII grammar so
// labels remain safe and stable across every boundary.
func TestValidUsesSharedLowercaseASCIIGrammar(t *testing.T) {
	t.Parallel()
	for _, value := range []string{"a", "runner-01", strings.Repeat("a", 63)} {
		if !Valid(value) {
			t.Fatalf("Valid(%q) = false", value)
		}
	}
	for _, value := range []string{
		"", "Runner", "runner/name", "runner_name", "runner--name", "-runner", "runner-",
		"runnér", "runner\x00name", strings.Repeat("a", 64),
	} {
		if Valid(value) {
			t.Fatalf("Valid(%q) = true", value)
		}
	}
}

func TestValidateClassifiesTheCanonicalGrammar(t *testing.T) {
	// Rationale: every product boundary must use the same grammar and error
	// classification rather than maintaining a local copy of the parser.
	t.Parallel()
	if err := Validate("tenant slug", "tenant-one"); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	err := Validate("tenant slug", "Tenant/One")
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Validate() error = %v", err)
	}
}
