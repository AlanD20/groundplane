package componentrender

import (
	"crypto/sha256"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func BuildPinnedEnvironmentComponentAction(
	catalog []EnvironmentComponentRegistration,
	componentID string,
	definitionDigest [sha256.Size]byte,
	catalogDigest [sha256.Size]byte,
	actionID componentsdk.ActionID,
	artifactID string,
	artifactDigest [sha256.Size]byte,
	generation uint64,
) (*agentpb.ComponentApply, error) {
	if err := ValidateEnvironmentComponentCatalog(catalog); err != nil {
		return nil, err
	}
	for _, registration := range catalog {
		if registration.Definition.Digest() != definitionDigest || registration.CatalogDigest != catalogDigest {
			continue
		}
		return BuildEnvironmentComponentAction(
			catalog, registration.Kind, componentID, actionID, artifactID, artifactDigest, generation,
		)
	}
	return nil, errs.New(errs.KindStateConflict, "pinned Environment Component definition is not registered")
}

func ManagedConfigurationIdentity(
	registration EnvironmentComponentRegistration,
) (string, componentsdk.ActionID, bool) {
	if registration.ManagedConfiguration == nil {
		return "", "", false
	}
	return registration.ManagedConfiguration.SourcePath, registration.ManagedConfiguration.ActionID, true
}

func BuildEnvironmentComponentAction(
	catalog []EnvironmentComponentRegistration,
	kind core.ComponentKind,
	componentID string,
	actionID componentsdk.ActionID,
	artifactID string,
	artifactDigest [sha256.Size]byte,
	generation uint64,
) (*agentpb.ComponentApply, error) {
	if err := ValidateEnvironmentComponentCatalog(catalog); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindComponent, componentID) != nil ||
		ids.Validate(ids.KindConfig, artifactID) != nil || generation == 0 ||
		zeroComponentDigest(artifactDigest) {
		return nil, errs.New(errs.KindInternal, "Environment Component action identity is invalid")
	}
	for _, registration := range catalog {
		if registration.Kind != kind {
			continue
		}
		action, found := registration.Definition.FindAction(actionID)
		if !found || action.Capability() != componentsdk.CapabilityManagedConfig ||
			action.Operation() != componentsdk.OperationActivate {
			return nil, errs.New(errs.KindInternal, "Environment Component action is not registered")
		}
		definitionDigest := registration.Definition.Digest()
		return &agentpb.ComponentApply{
			ComponentId:      componentID,
			DefinitionDigest: append([]byte(nil), definitionDigest[:]...),
			CatalogDigest:    append([]byte(nil), registration.CatalogDigest[:]...),
			ActionId:         string(actionID), ArtifactId: artifactID,
			ArtifactDigest: append([]byte(nil), artifactDigest[:]...),
			Generation:     generation,
		}, nil
	}
	return nil, errs.New(errs.KindInternal, "Environment Component action kind is not registered")
}

func zeroComponentDigest(value [sha256.Size]byte) bool {
	var combined byte
	for _, part := range value {
		combined |= part
	}
	return combined == 0
}
