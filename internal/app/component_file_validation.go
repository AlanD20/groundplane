package app

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/agent/componentfilevalidation"
)

func newComponentFileValidator(
	catalog registeredActionCatalog, executor componentfilevalidation.Executor,
) (*componentfilevalidation.Validator, error) {
	return componentfilevalidation.New(
		func(envelope componentsdk.ActionEnvelope) (componentfilevalidation.Recipe, error) {
			_, _, recipe, err := catalog.ResolveContainerConfigActionEnvelope(envelope)
			return componentfilevalidation.Recipe{
				Path: recipe.RelativePath(), Image: recipe.Image(), Arguments: recipe.PreflightArgs(),
			}, err
		},
		executor,
	)
}
