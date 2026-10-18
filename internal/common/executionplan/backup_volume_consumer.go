package executionplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BackupConsumer selects the exact acknowledged workload, independently
// of the desired artifact that proves the Volume's identity. Neither authority
// substitutes for the other; callers cannot select an arbitrary Compose name.
func BackupConsumer(environmentID string, services []*agentpb.BackupServiceFact,
	artifacts []*agentpb.ComposeArtifact, step *agentpb.BackupStepAuthority,
	serviceID string,
) (*agentpb.ComposeArtifact, *agentpb.ComposeService, error) {
	if environmentID == "" || step == nil || !slices.Contains(step.ConsumerServiceIds, serviceID) {
		return nil, nil, invalidBackupVolumeArtifact()
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range services {
		if candidate.GetServiceId() == serviceID {
			if fact != nil {
				return nil, nil, invalidBackupVolumeArtifact()
			}
			fact = candidate
		}
	}
	if fact == nil || len(fact.LocalImageIdSha256) != sha256.Size {
		return nil, nil, invalidBackupVolumeArtifact()
	}
	var artifact *agentpb.ComposeArtifact
	var service *agentpb.ComposeService
	for _, candidate := range artifacts {
		if candidate.GetOwnerId() != environmentID {
			continue
		}
		for _, member := range candidate.Services {
			if member.GetServiceId() != serviceID || member.GetComposeName() != fact.CurrentName ||
				(member.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON &&
					member.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT) {
				continue
			}
			labels, err := backupservicefact.LabelsDigest(member.ExpectedLabels)
			imageMatches := member.ImageReference == "sha256:"+hex.EncodeToString(fact.LocalImageIdSha256)
			if step.GetRestore().GetPostgres() != nil || step.GetRestore().GetMysql() != nil {
				// Database captures retain the applied artifact's reference. The
				// helper pins its command input to the separately captured local ID.
				imageMatches = backupDatabaseArtifactImage(member, fact)
			}
			if err != nil || uint32(len(member.ExpectedLabels)) != fact.RequiredLabelCount ||
				!bytes.Equal(labels, fact.RequiredLabelsSha256) ||
				!imageMatches || service != nil {
				return nil, nil, invalidBackupVolumeArtifact()
			}
			artifact, service = candidate, member
		}
	}
	if service == nil {
		return nil, nil, invalidBackupVolumeArtifact()
	}
	return artifact, service, nil
}
