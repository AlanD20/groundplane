package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

func TestValidateEnvironmentBlueprintAvailabilityAllowsBackup(t *testing.T) {
	parsed := blueprintparser.Result{
		Project:    &composetypes.Project{},
		Extensions: blueprintparser.Extensions{Backup: &core.BackupSpec{Enabled: false}},
	}
	if err := ValidateEnvironmentBlueprintAvailability(parsed); err != nil {
		t.Fatalf("ValidateEnvironmentBlueprintAvailability(Backup) error = %v", err)
	}
}
