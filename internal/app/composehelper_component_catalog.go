package app

import (
	componentaction "github.com/AlanD20/groundplane/internal/agent/componentaction"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"runtime"

	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type composeHelperComponentCatalog struct {
	catalog componentregistration.Catalog
}

func (catalog composeHelperComponentCatalog) ResolveContainerConfigAction(
	action *agentpb.ComponentApply,
) (composehelper.ComponentActionRecipe, error) {
	envelope, err := componentaction.DecodeComponentAction(action)
	if err != nil {
		return composehelper.ComponentActionRecipe{}, err
	}
	_, _, recipe, err := catalog.catalog.ResolveContainerConfigActionEnvelope(envelope)
	if err != nil {
		return composehelper.ComponentActionRecipe{}, err
	}
	platform, reference, _ := recipe.Image().Select(runtime.GOOS, runtime.GOARCH)
	return composehelper.NewComponentActionRecipe(
		recipe.RelativePath(), recipe.ContainerPath(), reference, platform,
		recipe.ValidateArgs(), recipe.ActivateArgs(),
	)
}
