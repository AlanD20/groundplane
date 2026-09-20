package executionplan

import (
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateMaterializeFile(
	renderGeneration uint64,
	materialization *agentpb.MaterializeFile,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if materialization == nil || validateID(ids.KindConfig, materialization.MaterializationId) != nil ||
		validateID(ids.KindEnvironment, materialization.EnvironmentId) != nil ||
		len(materialization.Sha256) != sha256.Size ||
		materialization.Length > entrymaterialization.MaximumContentBytes ||
		uint64(len(materialization.Destination)) > uint64(entrymaterialization.MaximumDestinationBytes) {
		return errs.New(errs.KindValidationFailed, "materialization metadata is invalid")
	}
	artifact := artifacts[materialization.ArtifactId]
	if artifact == nil || artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.OwnerId != materialization.EnvironmentId {
		return errs.New(errs.KindValidationFailed, "materialization artifact ownership is invalid")
	}
	serviceName := ""
	if materialization.ServiceId != "" {
		for _, service := range artifact.Services {
			if service.ServiceId == materialization.ServiceId {
				serviceName = service.ComposeName
				break
			}
		}
		if serviceName == "" || materialization.ServiceName != serviceName {
			return errs.New(errs.KindValidationFailed, "materialization service identity is invalid")
		}
	} else if materialization.ServiceName != "" {
		return errs.New(errs.KindValidationFailed, "materialization service identity is invalid")
	}
	outputKind, err := materializationOutputKind(materialization.OutputKind)
	if err != nil {
		return err
	}
	if err := entrymaterialization.ValidateMetadata(entrymaterialization.MetadataSpec{
		EnvironmentID: materialization.EnvironmentId,
		Generation:    renderGeneration,
		Destination:   materialization.Destination,
		ServiceID:     materialization.ServiceId,
		ServiceName:   materialization.ServiceName,
		OutputKind:    outputKind,
		UID:           materialization.Uid,
		GID:           materialization.Gid,
		Mode:          entrymaterialization.Mode(materialization.Mode),
	}); err != nil {
		return errs.New(errs.KindValidationFailed, "materialization output policy is invalid")
	}
	return nil
}

func materializationOutputKind(value agentpb.MaterializationOutputKind) (entrymaterialization.OutputKind, error) {
	switch value {
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_GENERATED_ENV:
		return entrymaterialization.OutputGeneratedEnv, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE:
		return entrymaterialization.OutputPlainFile, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_SECRET_FILE:
		return entrymaterialization.OutputSecretFile, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_GENERATED_ENV:
		return entrymaterialization.OutputRemoveGeneratedEnv, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_PLAIN_FILE:
		return entrymaterialization.OutputRemovePlainFile, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_SECRET_FILE:
		return entrymaterialization.OutputRemoveSecretFile, nil
	default:
		return 0, errs.New(errs.KindValidationFailed, "materialization output kind is invalid")
	}
}
