package handlers

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestScriptListRequestRequiresStableEnvironment(t *testing.T) {
	// Rationale: slug-scoped reads would make pagination and tenant isolation ambiguous.
	if _, err := scriptListRequest("production", 0, ""); err == nil {
		t.Fatal("scriptListRequest() accepted an Environment label")
	}
	if _, err := scriptListRequest(ids.New(ids.KindEnvironment), 200, ""); err != nil {
		t.Fatalf("scriptListRequest() stable id: %v", err)
	}
}
