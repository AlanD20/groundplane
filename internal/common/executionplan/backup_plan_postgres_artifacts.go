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
	return markBackupDatabaseArtifacts(plan, step, artifacts, used, databaseID,
		func(service *agentpb.ComposeService, fact *agentpb.BackupServiceFact) bool {
			return backupPostgresArtifactImage(service, fact, release, true)
		})
}

func markBackupMySQLArtifacts(plan *agentpb.ExecutionPlan, step *agentpb.BackupStepAuthority,
	artifacts map[string]*agentpb.ComposeArtifact, used map[string]bool,
) error {
	mysql := step.GetCapture().GetMysql()
	if restore := step.GetRestore().GetMysql(); restore != nil {
		mysql = &agentpb.BackupMySQLCaptureAuthority{DatabaseServiceId: restore.DatabaseServiceId,
			DatabaseImageReferenceSha256: restore.DatabaseImageReferenceSha256}
	}
	if mysql == nil {
		return nil
	}
	return markBackupDatabaseArtifacts(plan, step, artifacts, used, mysql.DatabaseServiceId,
		func(service *agentpb.ComposeService, fact *agentpb.BackupServiceFact) bool {
			digest := sha256.Sum256([]byte(service.ImageReference))
			return service.PostgresToolsImage == "" && bytes.Equal(digest[:], mysql.DatabaseImageReferenceSha256)
		})
}

func markBackupDatabaseArtifacts(plan *agentpb.ExecutionPlan, step *agentpb.BackupStepAuthority,
	artifacts map[string]*agentpb.ComposeArtifact, used map[string]bool, databaseID string,
	databaseImage func(*agentpb.ComposeService, *agentpb.BackupServiceFact) bool,
) error {
	serviceIDs := append([]string{databaseID}, step.ConsumerServiceIds...)
	for _, serviceID := range serviceIDs {
		fact := backupScopeServiceFact(plan.BackupScope, serviceID)
		if fact == nil || len(fact.LocalImageIdSha256) != sha256.Size {
			return invalidBackupDatabaseArtifact()
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
					if candidate.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED {
						if _, err := backingruntimefact.SelectWorkload(artifact, serviceID); err != nil {
							return invalidBackupDatabaseArtifact()
						}
					} else if candidate.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
						return invalidBackupDatabaseArtifact()
					}
				} else if candidate.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON &&
					candidate.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
					continue
				}
				digest, err := backupservicefact.LabelsDigest(candidate.ExpectedLabels)
				imageMatches := backupDatabaseArtifactImage(candidate, fact)
				if serviceID == databaseID {
					imageMatches = imageMatches && databaseImage(candidate, fact)
				}
				if err == nil && imageMatches &&
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
				return invalidBackupDatabaseArtifact()
			}
			selected = id
		}
		if selected == "" {
			return invalidBackupDatabaseArtifact()
		}
		used[selected] = true
	}
	return nil
}

func backupDatabaseArtifactImage(service *agentpb.ComposeService, fact *agentpb.BackupServiceFact) bool {
	return service.ImageReference != "" && (len(service.ImageConfigDigest) == 0 ||
		bytes.Equal(service.ImageConfigDigest, fact.LocalImageIdSha256))
}

func backupPostgresArtifactImage(service *agentpb.ComposeService, fact *agentpb.BackupServiceFact,
	release postgres16protocol.ManagedReleaseIndex, database bool,
) bool {
	if !backupDatabaseArtifactImage(service, fact) {
		return false
	}
	if !database {
		// The acknowledged artifact pins the authored reference; execution also
		// proves the actual container's local image ID against this sealed fact.
		return true
	}
	imageID := "sha256:" + hex.EncodeToString(fact.LocalImageIdSha256)
	return service.PostgresToolsImage == release.Image &&
		(postgres16protocol.ValidDatabaseImage(service.ImageReference) || service.ImageReference == imageID)
}

func invalidBackupPostgresArtifact() error {
	return invalidBackupDatabaseArtifact()
}

func invalidBackupDatabaseArtifact() error {
	return errs.New(
		errs.KindValidationFailed,
		"database operation requires exact applied database and consumer artifacts",
	)
}
