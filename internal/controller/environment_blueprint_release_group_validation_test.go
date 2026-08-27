package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: the implemented Release Group contract must remain reachable through Environment Blueprint availability validation.
func TestValidateEnvironmentBlueprintAvailabilityAllowsReleaseGroups(t *testing.T) {
	t.Parallel()
	result := blueprintparser.Result{
		Project: &composetypes.Project{},
		Extensions: blueprintparser.Extensions{
			ReleaseGroups: map[string]core.ReleaseGroupSpec{
				"web": {Services: []string{"api", "worker"}},
			},
		},
	}
	if err := ValidateEnvironmentBlueprintAvailability(result); err != nil {
		t.Fatalf("ValidateEnvironmentBlueprintAvailability() error = %v", err)
	}
}
