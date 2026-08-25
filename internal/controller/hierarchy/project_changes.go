package hierarchy

import (
	sluggrammar "github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// EditProjectInput is deliberately closed to the one Console edit action.
// Description is create-time-only and slug changes use RenameProjectInput.
type EditProjectInput struct {
	Name *string
}

type RenameProjectInput struct {
	Slug string
}

func ValidateProjectEditInput(input EditProjectInput) error {
	if input.Name == nil {
		return errs.New(errs.KindValidationFailed, "Project edit requires name")
	}
	return validateText("project name", *input.Name)
}

func PrepareProjectEdit(current core.Project, input EditProjectInput) (core.Project, error) {
	if current.Kind != core.ProjectKindTenant {
		return core.Project{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if err := ValidateProjectEditInput(input); err != nil {
		return core.Project{}, err
	}
	replacement := current
	replacement.Name = *input.Name
	return replacement, nil
}

func ValidateProjectRenameInput(input RenameProjectInput) error {
	return sluggrammar.Validate("project slug", input.Slug)
}

func PrepareProjectRename(current core.Project, input RenameProjectInput) (core.Project, error) {
	if current.Kind != core.ProjectKindTenant {
		return core.Project{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if err := ValidateProjectRenameInput(input); err != nil {
		return core.Project{}, err
	}
	replacement := current
	replacement.Slug = input.Slug
	return replacement, nil
}
