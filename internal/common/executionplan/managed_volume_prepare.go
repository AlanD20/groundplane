package executionplan

import (
	"crypto/sha256"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateManagedVolumeDirectoriesEnsure(
	operation agentpb.PlanOperation,
	ensure *agentpb.ManagedVolumeDirectoriesEnsure,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if ensure == nil || !operationCreatesManagedVolumes(operation) || len(ensure.IntentSha256) != sha256.Size ||
		len(ensure.VolumeIds) == 0 {
		return errs.New(errs.KindValidationFailed, "managed volume directory ensure payload is invalid")
	}
	artifact := artifacts[ensure.ArtifactId]
	if artifact == nil || artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		len(artifact.Volumes) == 0 {
		return errs.New(errs.KindValidationFailed, "managed volume directory artifact is invalid")
	}
	available := make(map[string]struct{}, len(artifact.Volumes))
	for _, volume := range artifact.Volumes {
		available[volume.VolumeId] = struct{}{}
	}
	previous := ""
	for _, volumeID := range ensure.VolumeIds {
		if validateID(ids.KindVolume, volumeID) != nil || volumeID <= previous {
			return errs.New(errs.KindValidationFailed, "managed volume directory ids are invalid or unsorted")
		}
		if _, exists := available[volumeID]; !exists {
			return errs.New(errs.KindValidationFailed, "managed volume directory id is absent from its artifact")
		}
		previous = volumeID
	}
	return nil
}

func validateManagedVolumeEnsure(
	operation agentpb.PlanOperation,
	ensure *agentpb.ManagedVolumeEnsure,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if ensure == nil || operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		validateID(ids.KindVolume, ensure.GetVolumeId()) != nil {
		return errs.New(errs.KindValidationFailed, "managed volume preparation payload is invalid")
	}
	artifact := artifacts[ensure.GetArtifactId()]
	if artifact == nil || artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetAuthorizedVolumeDir() == "" {
		return errs.New(errs.KindValidationFailed, "managed volume preparation requires an Environment artifact")
	}
	for _, volume := range artifact.GetVolumes() {
		if volume.GetVolumeId() == ensure.GetVolumeId() && validManagedVolumeComposeKey(volume.GetComposeName()) &&
			volume.GetDockerName() == "gp_vol_"+strings.ToLower(volume.GetVolumeId()) {
			return nil
		}
	}
	return errs.New(errs.KindValidationFailed, "managed volume preparation selection is absent or invalid")
}
