package valkey9

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/internal/adapters"
)

// Rationale: Valkey ACL credentials must be sent as mutable stdin to the
// compiled binary, never exposed in Docker exec arguments.
func TestProvisionStepsKeepPasswordOutOfArguments(t *testing.T) {
	steps := (&adapter{}).ProvisionSteps(adapters.ProvisionParams{
		Role: "api_5d3f9a", Password: []byte("URL_safe-1"),
	})
	if len(steps) != 1 || steps[0].Program != "valkey-cli" || len(steps[0].Args) != 0 ||
		!bytes.Contains(steps[0].Stdin, []byte(">URL_safe-1")) {
		t.Fatalf("ProvisionSteps() = %#v", steps)
	}
}
