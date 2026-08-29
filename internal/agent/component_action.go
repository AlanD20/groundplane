package agent

import (
	"context"
	"crypto/sha256"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ComponentActionRuntime interface {
	ExecuteComponentAction(context.Context, Assignment, *agentpb.ExecutionStep, ManagedConfigPayload) error
}

func DecodeComponentAction(payload *agentpb.ComponentApply) (componentsdk.ActionEnvelope, error) {
	if payload == nil || len(payload.DefinitionDigest) != sha256.Size ||
		len(payload.CatalogDigest) != sha256.Size || len(payload.ArtifactDigest) != sha256.Size {
		return componentsdk.ActionEnvelope{}, errs.New(
			errs.KindValidationFailed,
			"agent: Component action payload is invalid",
		)
	}
	componentID, err := componentsdk.NewComponentID(payload.ComponentId)
	if err != nil {
		return componentsdk.ActionEnvelope{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	artifactID, err := componentsdk.NewArtifactID(payload.ArtifactId)
	if err != nil {
		return componentsdk.ActionEnvelope{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	artifact, err := componentsdk.NewArtifactReference(
		artifactID,
		bytesToComponentDigest(payload.ArtifactDigest),
	)
	if err != nil {
		return componentsdk.ActionEnvelope{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	envelope, err := componentsdk.NewActionEnvelope(componentsdk.ActionEnvelopeInput{
		ComponentID: componentID,
		DefinitionDigest: bytesToComponentDigest(payload.DefinitionDigest),
		CatalogDigest: bytesToComponentDigest(payload.CatalogDigest),
		ActionID: componentsdk.ActionID(payload.ActionId), Artifact: artifact,
		Generation: payload.Generation,
	})
	if err != nil {
		return componentsdk.ActionEnvelope{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	return envelope, nil
}

func bytesToComponentDigest(value []byte) [sha256.Size]byte {
	var digest [sha256.Size]byte
	copy(digest[:], value)
	return digest
}
