package app

import (
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
	return composehelper.NewComponentActionRecipe(
		recipe.RelativePath(), recipe.ContainerPath(), recipe.ImageReference(),
		recipe.ValidateArgs(), recipe.ActivateArgs(),
	)
}
