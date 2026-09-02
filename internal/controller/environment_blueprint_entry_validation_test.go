package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func TestValidateEnvironmentBlueprintAvailabilityAllowsEntries(t *testing.T) {
	parsed := blueprintparser.Result{
		Project: &composetypes.Project{},
		Extensions: blueprintparser.Extensions{Entries: map[string]core.EntrySpec{
			"APP_MODE": {
				Kind: core.EntryKindEnv, Source: core.EntrySourceSpec{Literal: "acceptance"},
				Exposure: []string{"all"},
			},
		}},
	}
	if err := ValidateEnvironmentBlueprintAvailability(parsed); err != nil {
		t.Fatalf("ValidateEnvironmentBlueprintAvailability() error = %v", err)
	}
}

func TestValidateEnvironmentBlueprintAvailabilityAllowsRequires(t *testing.T) {
	t.Parallel()
	parsed := blueprintparser.Result{
		Project: &composetypes.Project{},
		Extensions: blueprintparser.Extensions{Requires: []core.Requirement{{
			Target:    core.RequirementTarget{Kind: core.RequirementTargetBackingAttach, Name: "api-db"},
			Condition: core.RequirementReady,
			Phases:    []core.RequirementPhase{core.RequirementPhaseDeploy},
		}}},
	}
	if err := ValidateEnvironmentBlueprintAvailability(parsed); err != nil {
		t.Fatalf("ValidateEnvironmentBlueprintAvailability() error = %v", err)
	}
}
