package app

import (
	"runtime"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type composeHelperComponentCatalog struct {
	catalog registeredActionCatalog
}

func (catalog composeHelperComponentCatalog) ResolveContainerConfigAction(
	action *agentpb.ComponentApply,
) (composehelper.ComponentActionRecipe, error) {
	envelope, err := agent.DecodeComponentAction(action)
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
