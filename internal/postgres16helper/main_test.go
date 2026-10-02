package postgres16helper

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
)

// Rationale: a helper built without authenticated release measurements must
// reject execution before opening a client or accepting a public request.
func TestMainRejectsMissingReleaseMeasurements(t *testing.T) {
	code, err := Main(context.Background(), []string{"run"})
	if code != postgres16protocol.ExitEnvironmentInvalid || err == nil {
		t.Fatalf("Main() = %d, %v; want environment_invalid and an error", code, err)
	}
}
