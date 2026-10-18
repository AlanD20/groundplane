package taskplanning

import (
	"bytes"

	registeredcatalog "github.com/AlanD20/groundplane-component-sdk/catalog"
	"github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func containerActivation(catalog []componentrender.EnvironmentComponentRegistration, action *agentpb.ComponentApply) (
	registeredcatalog.ContainerConfigActivation, string, core.ComponentKind, error,
) {
	if action == nil {
		return "", "", "", errs.New(errs.KindInternal, "container Component action resolver is missing")
	}
	for _, registration := range catalog {
		managed := registration.ManagedConfiguration
		definitionDigest := registration.Definition.Digest()
		if managed != nil && string(managed.ActionID) == action.GetActionId() &&
			bytes.Equal(definitionDigest[:], action.GetDefinitionDigest()) &&
			bytes.Equal(registration.CatalogDigest[:], action.GetCatalogDigest()) {
			return managed.Activation, managed.SourcePath, registration.Kind, nil
		}
	}
	return "", "", "", errs.New(errs.KindStateConflict, "container Component action is not in the captured catalog")
}
