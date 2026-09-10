// Package componentfilevalidation validates a sealed Component's native file
// before its ordinary Environment materialization can change serving bytes.
package componentfilevalidation

import (
	"context"
	"runtime"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelpercontainer"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Recipe struct {
	Path      string
	Image     componentsdk.OCIImage
	Arguments []string
}

type RecipeResolver func(componentsdk.ActionEnvelope) (Recipe, error)

type Executor interface {
	ValidateToExit(context.Context, managedconfighelpercontainer.ValidatorImage, []string, []byte) error
}

type Validator struct {
	resolve RecipeResolver
	execute Executor
}

func New(resolve RecipeResolver, execute Executor) (*Validator, error) {
	if resolve == nil || execute == nil {
		return nil, errs.New(errs.KindInternal, "Component file validator dependencies are required")
	}
	return &Validator{resolve: resolve, execute: execute}, nil
}

func (validator *Validator) ValidateComponentFile(
	ctx context.Context, envelope componentsdk.ActionEnvelope, destination string, content []byte,
) error {
	if ctx == nil || validator == nil || validator.resolve == nil || validator.execute == nil {
		return errs.New(errs.KindInternal, "Component file validator is not configured")
	}
	recipe, err := validator.resolve(envelope)
	if err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	platform, reference, found := recipe.Image.Select(runtime.GOOS, runtime.GOARCH)
	if destination == "" || recipe.Path != destination || recipe.Image.Validate() != nil || !found ||
		len(recipe.Arguments) == 0 || len(content) == 0 {
		return errs.New(errs.KindValidationFailed, "Component file preflight recipe does not match its materialization")
	}
	return validator.execute.ValidateToExit(ctx, managedconfighelpercontainer.ValidatorImage{
		Reference: reference, Platform: platform,
	}, append([]string(nil), recipe.Arguments...), content)
}
