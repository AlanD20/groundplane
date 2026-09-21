package backuppolicymutations

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *Repository) loadBackupPolicyReplacementBase(
	ctx context.Context,
	input backuppolicy.BackupPolicyReplacementInput,
	projectID string,
	tenantID string,
	now time.Time,
) (ReplacementCandidate, bool, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		hierarchyrecord.EnvironmentKey(input.EnvironmentID),
		hierarchyrecord.ProjectKey(projectID),
		backuppolicy.BackupPolicyKey(input.EnvironmentID),
		hierarchyrecord.EnvironmentMutationEpochKey(input.EnvironmentID),
		hierarchyrecord.EnvironmentOperationLockKey(input.EnvironmentID),
		backuppolicy.BackupKeyKey(input.EnvironmentID),
		backuppolicy.BackupKeyValueKey(input.EnvironmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), input.EnvironmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), projectID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), tenantID),
		coordinationrecord.Key(input.EnvironmentID),
	}})
	if err != nil {
		return ReplacementCandidate{}, false, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != 11 {
		return ReplacementCandidate{}, false, errs.New(
			errs.KindInternal,
			"backup policy replacement base read is incomplete",
		)
	}
	defer etcdstore.ClearValues(result.Values)
	if result.Values[0] == nil {
		return ReplacementCandidate{}, false, errs.New(
			errs.KindEnvironmentNotFound,
			"environment was not found",
		)
	}
	environment, err := hierarchyrecord.DecodeEnvironment(result.Values[0].Value)
	if err != nil || environment.ID != input.EnvironmentID || environment.ProjectID != projectID {
		return ReplacementCandidate{}, false, recordcodec.CorruptRecord()
	}
	if environment.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return ReplacementCandidate{}, false, errs.New(
			errs.KindStateConflict,
			"environment is not ready for backup policy replacement",
		)
	}
	if result.Values[1] == nil {
		return ReplacementCandidate{}, false, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	project, err := hierarchyrecord.DecodeProject(result.Values[1].Value)
	if err != nil || project.ID != projectID || project.TenantID != tenantID ||
		project.Kind != hierarchyrecord.ProjectKindTenant {
		return ReplacementCandidate{}, false, recordcodec.CorruptRecord()
	}
	for _, index := range []int{7, 8, 9} {
		if result.Values[index] != nil {
			return ReplacementCandidate{}, false, errs.New(
				errs.KindResourceInUse,
				"backup policy hierarchy deletion is in progress",
			)
		}
	}
	if result.Values[4] != nil {
		return ReplacementCandidate{}, false, errs.New(
			errs.KindResourceInUse,
			"environment persistence operation is in progress",
		)
	}
	if result.Values[3] == nil {
		return ReplacementCandidate{}, false, errs.New(
			errs.KindInternal,
			"environment mutation epoch is missing",
		)
	}
	epoch, err := backupruntime.DecodeEnvironmentMutationEpochRecord(result.Values[3].Value)
	if err != nil || epoch.EnvironmentID != input.EnvironmentID {
		return ReplacementCandidate{}, false, errs.New(
			errs.KindInternal,
			"environment mutation epoch is corrupt",
		)
	}
	candidate := ReplacementCandidate{
		Environment: etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{
			Record: environment, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
		},
		Project: etcdstore.Versioned[hierarchyrecord.ProjectRecord]{
			Record: project, Revision: result.Values[1].ModRevision, ReadRevision: result.ReadRevision,
		},
		MutationEpoch: etcdstore.Versioned[backupruntime.EnvironmentMutationEpochRecord]{
			Record: epoch, Revision: result.Values[3].ModRevision, ReadRevision: result.ReadRevision,
		},
		Replacement: backuppolicy.BackupPolicyRecord{
			EnvironmentID: input.EnvironmentID,
			Enabled:       input.Enabled,
			Frequency:     input.Frequency,
			Keep:          input.Keep,
			Encryption:    input.Encryption,
			ConnectorID:   input.ConnectorID,
			SourceIDs:     make([]string, len(input.Sources)),
			UpdatedAt:     now,
		},
		Sources: make([]SourceEvidence, len(input.Sources)),
	}
	if result.Values[2] != nil {
		current, decodeErr := backuppolicy.DecodeBackupPolicyRecord(result.Values[2].Value)
		if decodeErr != nil || current.EnvironmentID != input.EnvironmentID {
			return ReplacementCandidate{}, false, recordcodec.CorruptRecord()
		}
		candidate.Current = &etcdstore.Versioned[backuppolicy.BackupPolicyRecord]{
			Record: current, Revision: result.Values[2].ModRevision, ReadRevision: result.ReadRevision,
		}
	}
	coordination := coordinationrecord.EnvironmentCoordinationRecord{
		EnvironmentID: input.EnvironmentID, ScheduleClockFloor: now,
	}
	coordinationRevision := int64(0)
	if result.Values[10] != nil {
		decoded, coordinationErr := coordinationrecord.Decode(result.Values[10].Value)
		if coordinationErr != nil || decoded.EnvironmentID != input.EnvironmentID {
			return ReplacementCandidate{}, false, coordinationrecord.CorruptRecord()
		}
		coordination = decoded
		coordinationRevision = result.Values[10].ModRevision
	} else if candidate.Current != nil {
		return ReplacementCandidate{}, false, coordinationrecord.CorruptRecord()
	}
	candidate.Coordination = etcdstore.Versioned[coordinationrecord.EnvironmentCoordinationRecord]{
		Record: coordination, Revision: coordinationRevision, ReadRevision: result.ReadRevision,
	}
	if candidate.Current == nil {
		if coordination.CurrentBackupScheduleState != nil {
			return ReplacementCandidate{}, false, coordinationrecord.CorruptRecord()
		}
	} else if candidate.Current.Record.Enabled {
		digest, digestErr := coordinationrecord.PolicyScheduleDigest(candidate.Current.Record)
		state := coordination.CurrentBackupScheduleState
		if digestErr != nil || state == nil || state.PolicyDigest != digest ||
			state.Frequency != candidate.Current.Record.Frequency {
			return ReplacementCandidate{}, false, coordinationrecord.CorruptRecord()
		}
	} else if coordination.CurrentBackupScheduleState != nil {
		return ReplacementCandidate{}, false, coordinationrecord.CorruptRecord()
	}
	if (result.Values[5] == nil) != (result.Values[6] == nil) {
		return ReplacementCandidate{}, false, backuppolicy.CorruptBackupKey()
	}
	keyFound := result.Values[5] != nil
	if keyFound {
		record, decodeErr := backuppolicy.DecodeBackupKeyRecord(result.Values[5].Value)
		if decodeErr != nil {
			return ReplacementCandidate{}, false, backuppolicy.CorruptBackupKey()
		}
		encrypted, decodeErr := backuppolicy.DecodeBackupKeyEncryptedValue(result.Values[6].Value)
		if decodeErr != nil || record.EnvironmentID != input.EnvironmentID ||
			encrypted.EnvironmentID != input.EnvironmentID || record.KeyEra != encrypted.KeyEra {
			clear(encrypted.Ciphertext)
			return ReplacementCandidate{}, false, backuppolicy.CorruptBackupKey()
		}
		candidate.ExistingKey = &backuppolicy.VersionedBackupKey{
			Record: record, Encrypted: encrypted,
			RecordRevision: result.Values[5].ModRevision, EncryptedRevision: result.Values[6].ModRevision,
			ReadRevision: result.ReadRevision,
		}
	}
	return candidate, keyFound, nil
}
