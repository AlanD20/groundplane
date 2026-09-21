package backuppolicymutations

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func (repository *Repository) loadSourceEvidence(
	ctx context.Context,
	expected etcdstore.Versioned[backuppolicy.BackupSourceRecord],
	revision int64,
) (SourceEvidence, error) {
	record := expected.Record
	keys := []string{
		backuppolicy.BackupSourceKey(record.ID),
		backuppolicy.BackupSourceEnvironmentKey(record.EnvironmentID, record.ID),
		backuppolicy.BackupSourceIdentityKey(record.EnvironmentID, record.Kind, record.TargetID),
	}
	if record.Kind == core.BackupSourceAttach {
		keys = append(keys, attachrecord.AttachKey(record.TargetID), attachrecord.AttachOwnerKey(record.EnvironmentID, record.TargetID))
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return SourceEvidence{}, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) || result.Values[0] == nil ||
		result.Values[1] == nil || result.Values[2] == nil {
		return SourceEvidence{}, recordcodec.CorruptRecord()
	}
	current, err := backuppolicy.DecodeBackupSourceRecord(result.Values[0].Value)
	if err != nil || current.ID != record.ID || current.EnvironmentID != record.EnvironmentID ||
		current.Kind != record.Kind || current.TargetID != record.TargetID ||
		string(result.Values[1].Value) != record.ID || string(result.Values[2].Value) != record.ID {
		return SourceEvidence{}, recordcodec.CorruptRecord()
	}
	evidence := SourceEvidence{
		Source: etcdstore.Versioned[backuppolicy.BackupSourceRecord]{
			Record: current, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
		},
		EnvironmentIndex: CloneBackupPolicyEvidenceKeyValue(result.Values[1]),
		IdentityIndex:    CloneBackupPolicyEvidenceKeyValue(result.Values[2]),
	}
	if record.Kind == core.BackupSourceConfig {
		return evidence, nil
	}
	if record.Kind == core.BackupSourceVolume {
		volume, volumeErr := environmentqueries.LoadBackupVolumeProjectionEvidence(
			ctx, repository.store, record.EnvironmentID, record.TargetID, revision,
		)
		if volumeErr != nil {
			return SourceEvidence{}, volumeErr
		}
		evidence.Volume = &volume
		return evidence, nil
	}
	if result.Values[3] == nil || result.Values[4] == nil ||
		string(result.Values[4].Value) != record.TargetID {
		return SourceEvidence{}, recordcodec.CorruptRecord()
	}
	evidence.TargetOwnerIndex = CloneBackupPolicyEvidenceKeyValue(result.Values[4])
	if record.Kind == core.BackupSourceAttach {
		attach, decodeErr := attachrecord.DecodeAttachRecord(result.Values[3].Value)
		if decodeErr != nil || attach.ID != record.TargetID || attach.EnvironmentID != record.EnvironmentID {
			return SourceEvidence{}, recordcodec.CorruptRecord()
		}
		evidence.Attach = &etcdstore.Versioned[attachrecord.Record]{
			Record: attach, Revision: result.Values[3].ModRevision, ReadRevision: result.ReadRevision,
		}
	}
	return evidence, nil
}
