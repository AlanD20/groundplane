package executionplan

import (
	"bytes"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
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
			!bytes.Equal(fact.RequiredLabelsSha256, value.ObservedLabelsSha256) {
			continue
		}
		for _, image := range release.Images {
			manifest, manifestErr := hex.DecodeString(strings.TrimPrefix(image.ManifestDigest(), "sha256:"))
			imageID, imageErr := hex.DecodeString(strings.TrimPrefix(image.ImageID, "sha256:"))
			if manifestErr == nil && imageErr == nil && bytes.Equal(manifest, value.RepositoryDigest) &&
				bytes.Equal(imageID, fact.LocalImageIdSha256) {
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

func (replay *BackupStepResumeReplay) postgresCaptureCheckpoint(request *agentpb.BackupCheckpointRequest) error {
	if replay.step.GetCapture().GetPostgres() == nil || replay.prepared != nil || replay.uploaded != nil {
		return backupResumeHistoryInvalid()
	}
	if value := request.GetPostgresContainerObserved(); value != nil {
		if replay.sequence != 0 || replay.postgresObserved != nil ||
			!postgresObservationMatchesService(value, replay.step, replay.authority.Services) {
			return backupResumeHistoryInvalid()
		}
		replay.postgresObserved = value
		replay.capture.Checkpoint = &agentpb.BackupCaptureResume_PostgresContainerObserved{
			PostgresContainerObserved: value,
		}
		return nil
	}
	value, observed := request.GetPostgresDumpStart(), replay.postgresObserved
	if observed == nil || replay.postgresDumpStart != nil || !postgresDumpMatches(value, replay.step) ||
		value.ContainerId != observed.ContainerId || !bytes.Equal(value.RepositoryDigest, observed.RepositoryDigest) ||
		!bytes.Equal(value.ExpectedLabelsSha256, observed.ObservedLabelsSha256) {
		return backupResumeHistoryInvalid()
	}
	replay.postgresDumpStart = value
	replay.capture.DumpStart = proto.CloneOf(value)
	replay.capture.Checkpoint = &agentpb.BackupCaptureResume_PostgresDumpStart{PostgresDumpStart: value}
	return nil
}
