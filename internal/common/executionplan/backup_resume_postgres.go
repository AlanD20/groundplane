package executionplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func postgresObservationMatches(
	value *agentpb.BackupPostgresContainerObserved,
	step *agentpb.BackupStepAuthority,
) bool {
	if !validBackupPostgresObservation(value) {
		return false
	}
	catalog, serviceID := step.GetCapture().
		GetPostgres().
		GetManagedReleaseIndex(),
		step.GetCapture().
			GetPostgres().
			GetDatabaseServiceId()
	if restore := step.GetRestore().GetPostgres(); restore != nil {
		catalog, serviceID = restore.ManagedReleaseIndex, restore.DatabaseServiceId
	}
	if value.ServiceId != serviceID {
		return false
	}
	release, err := postgres16protocol.DecodeManagedReleaseIndex(catalog)
	if err != nil {
		return false
	}
	for _, image := range release.Images {
		digest, err := hex.DecodeString(strings.TrimPrefix(image.ManifestDigest(), "sha256:"))
		if err == nil && bytes.Equal(digest, value.RepositoryDigest) {
			return true
		}
	}
	return false
}

func postgresObservationMatchesService(value *agentpb.BackupPostgresContainerObserved,
	step *agentpb.BackupStepAuthority, services []*agentpb.BackupServiceFact,
) bool {
	if !postgresObservationMatches(value, step) {
		return false
	}
	catalog := step.GetCapture().GetPostgres().GetManagedReleaseIndex()
	if step.GetRestore().GetPostgres() != nil {
		catalog = step.GetRestore().GetPostgres().ManagedReleaseIndex
	}
	release, err := postgres16protocol.DecodeManagedReleaseIndex(catalog)
	if err != nil {
		return false
	}
	for _, fact := range services {
		if fact.ServiceId != value.ServiceId || fact.RequiredLabelCount != value.ObservedLabelCount ||
			!bytes.Equal(fact.RequiredLabelsSha256, value.ObservedLabelsSha256) ||
			!bytes.Equal(fact.LocalImageIdSha256, value.DatabaseImageIdSha256) {
			continue
		}
		for _, image := range release.Images {
			manifest, manifestErr := hex.DecodeString(strings.TrimPrefix(image.ManifestDigest(), "sha256:"))
			if manifestErr == nil && bytes.Equal(manifest, value.RepositoryDigest) &&
				len(fact.LocalImageIdSha256) == sha256.Size {
				return true
			}
		}
	}
	return false
}

func postgresDumpMatches(value *agentpb.BackupPostgresDumpStart, step *agentpb.BackupStepAuthority) bool {
	postgres := step.GetCapture().GetPostgres()
	return postgres != nil && validBackupPostgresDump(value) && value.PointId == step.GetCapture().PointId &&
		value.AdapterContractVersion == postgres.AdapterContractVersion && value.DatabaseName == postgres.DatabaseName &&
		value.RoleName == postgres.RoleName && value.MaxPlaintextBytes == postgres.MaxPlaintextBytes
}
