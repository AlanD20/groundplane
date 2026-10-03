package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Backup carries exact sealed Compose artifacts, never mutable lookups by ID.
// Volume projection authority and PostgreSQL Service facts select their uses.
func validateBackupPlanArtifacts(plan *agentpb.ExecutionPlan) error {
	if len(plan.Artifacts) > MaximumArtifacts {
		return invalidBackupVolumeArtifact()
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(plan.Artifacts))
	for _, artifact := range plan.Artifacts {
		if artifact == nil || ids.Validate(ids.KindConfig, artifact.ArtifactId) != nil ||
			artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			ids.Validate(ids.KindEnvironment, artifact.OwnerId) != nil || len(artifact.CanonicalYaml) == 0 ||
			len(artifact.CanonicalYaml) > MaximumArtifactYAMLBytes || len(artifact.YamlSha256) != sha256.Size ||
			RejectUnknown(artifact) != nil || artifacts[artifact.ArtifactId] != nil {
			return invalidBackupVolumeArtifact()
		}
		yaml := sha256.Sum256(artifact.CanonicalYaml)
		if !bytes.Equal(artifact.YamlSha256, yaml[:]) {
			return invalidBackupVolumeArtifact()
		}
		artifacts[artifact.ArtifactId] = artifact
	}
	used := make(map[string]bool, len(artifacts))
	for _, execution := range plan.Steps {
		step := execution.GetBackupStep()
		if err := markBackupPostgresArtifacts(plan, step, artifacts, used); err != nil {
			return err
		}
		var volume *agentpb.BackupVolumeProjectionAuthority
		var volumeID string
		if capture := step.GetCapture().GetVolume(); capture != nil {
			volume, volumeID = capture.Projection, capture.VolumeId
		}
		if restore := step.GetRestore().GetVolume(); restore != nil {
			volume, volumeID = restore.Projection, restore.VolumeId
		}
		if volume == nil {
			continue
		}
		artifact := artifacts[volume.ArtifactId]
		if artifact == nil || artifact.OwnerId != plan.TargetId ||
			artifact.AuthorizedVolumeDir != volume.AuthorizedVolumeDir ||
			!backupVolumeArtifactDigestMatches(artifact, volume.ArtifactSha256) {
			return invalidBackupVolumeArtifact()
		}
		used[volume.ArtifactId] = true
		matchedVolume := false
		for _, candidate := range artifact.Volumes {
			if candidate != nil && candidate.VolumeId == volumeID && candidate.ComposeName == volume.ComposeVolumeKey &&
				candidate.DockerName == volume.DockerVolumeName {
				matchedVolume = true
				break
			}
		}
		if !matchedVolume {
			return invalidBackupVolumeArtifact()
		}
		for _, serviceID := range step.ConsumerServiceIds {
			consumer, _, err := BackupVolumeConsumer(plan, step, serviceID)
			if err != nil {
				return err
			}
			used[consumer.ArtifactId] = true
		}
	}
	for id := range artifacts {
		if !used[id] {
			return invalidBackupVolumeArtifact()
		}
	}
	return nil
}

func backupVolumeArtifactDigestMatches(artifact *agentpb.ComposeArtifact, expected []byte) bool {
	if len(expected) != sha256.Size {
		return false
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil {
		return false
	}
	digest := sha256.Sum256(encoded)
	return bytes.Equal(expected, digest[:])
}

func backupScopeServiceFact(scope *agentpb.BackupPlanScope, serviceID string) *agentpb.BackupServiceFact {
	for _, fact := range scope.GetServices() {
		if fact.ServiceId == serviceID {
			return fact
		}
	}
	return nil
}

func invalidBackupVolumeArtifact() error {
	return errs.New(
		errs.KindValidationFailed,
		"Volume operation artifact differs from its sealed projection and consumers",
	)
}
