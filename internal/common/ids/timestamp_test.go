package ids

import (
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Timestamp is a public validation boundary and must preserve the
// repository-wide typed-error contract for malformed stable IDs.
func TestTimestampReturnsTypedValidationError(t *testing.T) {
	t.Parallel()

	_, err := Timestamp(KindService, "not-an-id")
	if err == nil {
		t.Fatal("Timestamp() error = nil, want validation error")
	}
	if got, ok := errs.KindOf(err); !ok || got != errs.KindValidationFailed {
		t.Fatalf("Timestamp() error kind = %v, %t; want %v, true", got, ok, errs.KindValidationFailed)
	}
}
