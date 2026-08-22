package hierarchy

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type CreateEnvironmentInput struct {
	ProjectID string
	Name      string
}

func ValidateEnvironmentCreateInput(input CreateEnvironmentInput) error {
	if err := validateStableID(ids.KindProject, input.ProjectID); err != nil {
		return errs.New(errs.KindValidationFailed, "environment project_id is invalid")
	}
	return validateSlug("environment name", input.Name)
}

type RenameEnvironmentInput struct {
	Name string
}

func ValidateEnvironmentRenameInput(input RenameEnvironmentInput) error {
	if err := validateSlug("environment name", input.Name); err != nil {
		return err
	}
	return nil
}

func PrepareEnvironmentRename(currentName string, input RenameEnvironmentInput) (string, error) {
	if currentName == "" {
		return "", errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	if err := ValidateEnvironmentRenameInput(input); err != nil {
		return "", err
	}
	return input.Name, nil
}
