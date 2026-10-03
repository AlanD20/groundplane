package executionplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func markBackupPostgresArtifacts(plan *agentpb.ExecutionPlan, step *agentpb.BackupStepAuthority,
	artifacts map[string]*agentpb.ComposeArtifact, used map[string]bool,
) error {
	databaseID := step.GetCapture().GetPostgres().GetDatabaseServiceId()
	catalog := step.GetCapture().GetPostgres().GetManagedReleaseIndex()
	if restore := step.GetRestore().GetPostgres(); restore != nil {
		databaseID = restore.DatabaseServiceId
		catalog = restore.ManagedReleaseIndex
	}
	if databaseID == "" {
		return nil
	}
	release, err := postgres16protocol.DecodeManagedReleaseIndex(catalog)
	if err != nil {
		return err
	}
	serviceIDs := append([]string{databaseID}, step.ConsumerServiceIds...)
	for _, serviceID := range serviceIDs {
		fact := backupScopeServiceFact(plan.BackupScope, serviceID)
		if fact == nil || len(fact.LocalImageIdSha256) != sha256.Size {
			return invalidBackupPostgresArtifact()
		}
		selected := ""
		for id, artifact := range artifacts {
			if serviceID != databaseID && artifact.OwnerId != plan.TargetId {
				continue
			}
			matches := 0
			for _, candidate := range artifact.Services {
				if candidate == nil || candidate.ServiceId != serviceID || candidate.ComposeName != fact.CurrentName {
					continue
				}
				if serviceID == databaseID {
					if _, err := backingruntimefact.SelectWorkload(artifact, serviceID); err != nil {
						return invalidBackupPostgresArtifact()
					}
				} else if candidate.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON &&
					candidate.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
					continue
				}
				digest, err := backupservicefact.LabelsDigest(candidate.ExpectedLabels)
				if err == nil && backupPostgresArtifactImage(candidate, fact, release, serviceID == databaseID) &&
					uint32(
						len(candidate.ExpectedLabels),
					) == fact.RequiredLabelCount && bytes.Equal(digest, fact.RequiredLabelsSha256) {
					matches++
				}
			}
			if matches == 0 {
				continue
			}
			if matches != 1 || selected != "" {
				return invalidBackupPostgresArtifact()
			}
			selected = id
		}
		if selected == "" {
			return invalidBackupPostgresArtifact()
		}
		used[selected] = true
	}
	return nil
}

func backupPostgresArtifactImage(service *agentpb.ComposeService, fact *agentpb.BackupServiceFact,
	release postgres16protocol.ManagedReleaseIndex, database bool,
) bool {
	if service.ImageReference == "" || len(service.ImageConfigDigest) != 0 &&
		!bytes.Equal(service.ImageConfigDigest, fact.LocalImageIdSha256) {
		return false
	}
	if !database {
		// The acknowledged artifact pins the authored reference; execution also
		// proves the actual container's local image ID against this sealed fact.
		return true
	}
	imageID := "sha256:" + hex.EncodeToString(fact.LocalImageIdSha256)
	for _, image := range release.Images {
		if release.MatchesRuntimeImageID(image, imageID) &&
			(service.ImageReference == release.Image || service.ImageReference == image.RepositoryDigest) {
			return true
		}
	}
	return false
}

func invalidBackupPostgresArtifact() error {
	return errs.New(
		errs.KindValidationFailed,
		"PostgreSQL operation requires exact applied database and consumer artifacts",
	)
}
