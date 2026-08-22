package hierarchy

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type CreateEnvironmentInput struct {
	ProjectID   string
	Name        string
	NetworkPool string
}

func ValidateEnvironmentCreateInput(input CreateEnvironmentInput) error {
	if err := validateStableID(ids.KindProject, input.ProjectID); err != nil {
		return errs.New(errs.KindValidationFailed, "environment project_id is invalid")
	}
	if err := validateSlug("environment name", input.Name); err != nil {
		return err
	}
	pool, err := ipam.ParseIPv4Prefix(input.NetworkPool)
	if err != nil || pool.String() != input.NetworkPool {
		return errs.New(errs.KindValidationFailed, "environment network_pool must be a canonical IPv4 CIDR")
	}
	return nil
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
