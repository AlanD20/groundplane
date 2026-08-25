package postgres16helper

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
)

// Rationale: removing the unsafe uid-70 runtime must leave an explicit stable
// fail-closed result rather than accidentally accepting any public request.
func TestMainFailsClosedUntilConfinementRuntimeExists(t *testing.T) {
	code, err := Main(context.Background(), []string{"run"})
	if code != postgres16protocol.ExitInternalFailure || err == nil {
		t.Fatalf("Main() = %d, %v; want internal_failure and an internal error", code, err)
	}
}
