package etcd

import (
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (reader *BackupSecretResolutionReader) decodeSourceDynamicEvidence(
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	evidence *BackupSecretResolutionEvidence,
	plan *agentpb.ExecutionPlan,
	stepIndex int,
	sourceIDs map[string]int,
) error {
	if evidence.Run == nil {
		return nil
	}
	if stepIndex >= len(evidence.Run.Sources) {
		return errs.New(errs.KindStateConflict, "backup source evidence is unavailable")
	}
	source := evidence.Run.Sources[stepIndex]
	value := result.Values[sourceIDs[source.SourceID]]
	if value == nil || value.ModRevision != source.SourceRevision {
		return errs.New(errs.KindStateConflict, "backup source evidence changed")
	}
	stored, err := backuppolicy.DecodeBackupSourceRecord(value.Value)
	if err != nil || stored.ID != source.SourceID || stored.EnvironmentID != evidence.Run.EnvironmentID ||
		string(stored.Kind) != string(source.Kind) || stored.TargetID != source.TargetID {
		return errs.New(errs.KindStateConflict, "backup source evidence changed")
	}
	evidence.Source = stored
	evidence.SourceRevision = value.ModRevision
	step := plan.Steps[stepIndex].GetBackupSourceCapture()
	if step == nil {
		return errs.New(errs.KindStateConflict, "backup source step evidence changed")
	}
	if err := reader.validateCaptureTargetEvidence(result, dynamic, evidence.Run, source, step); err != nil {
		return err
	}
	return nil
}

func (reader *BackupSecretResolutionReader) validateCaptureTargetEvidence(
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	run *backupruntime.BackupRunRecord,
	source backupruntime.BackupRunSourceAttemptRecord,
	step *agentpb.BackupSourceCapture,
) error {
	switch source.Kind {
	case BackupRuntimeSourceAttach:
		snapshot := source.Snapshot.Postgres
		if snapshot == nil {
			return errs.New(errs.KindInternal, "postgres backup source snapshot is corrupt")
		}
		keys := []*etcdstore.KeyValue{
			result.Values[dynamic.index[attachrecord.AttachKey(source.TargetID)]],
			result.Values[dynamic.index[attachrecord.AttachFactsKey(source.TargetID)]],
			result.Values[dynamic.index[hierarchyrecord.ProjectKey(snapshot.BackingProjectID)]],
			result.Values[dynamic.index[hierarchyrecord.EnvironmentKey(snapshot.BackingEnvironmentID)]],
			result.Values[dynamic.index[environmentBlueprintHeadKey(snapshot.BackingEnvironmentID)]],
		}
		return validateBackupPostgresPublicationEvidence(keys, source, *snapshot)
	case BackupRuntimeSourceVolume:
		snapshot := source.Snapshot.Volume
		if snapshot == nil {
			return errs.New(errs.KindInternal, "volume backup source snapshot is corrupt")
		}
		keys := make([]*etcdstore.KeyValue, 0, len(snapshot.Services)+3)
		keys = append(
			keys,
			result.Values[dynamic.index[hierarchyrecord.EnvironmentKey(snapshot.EnvironmentID)]],
			result.Values[dynamic.index[environmentBlueprintHeadKey(snapshot.EnvironmentID)]],
			result.Values[dynamic.index[environmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID)]],
		)
		for _, service := range snapshot.Services {
			keys = append(keys, result.Values[dynamic.index[servicerecord.ServiceRuntimeKey(service.ServiceID)]])
		}
		return validateBackupVolumePublicationEvidence(keys, source, *snapshot)
	case BackupRuntimeSourceConfig:
		snapshot := source.Snapshot.Config
		config := step.GetConfig()
		if snapshot == nil || config == nil || snapshot.ConfigSnapshotID != run.TaskID ||
			config.SnapshotRevision != uint64(snapshot.ReadRevision) {
			return errs.New(errs.KindStateConflict, "backup config snapshot revision changed")
		}
		target := result.Values[dynamic.index[hierarchyrecord.EnvironmentKey(run.EnvironmentID)]]
		if target == nil || target.ModRevision != source.TargetRevision {
			return errs.New(errs.KindStateConflict, "backup config target evidence changed")
		}
		return nil
	default:
		return errs.New(errs.KindInternal, "backup source kind is corrupt")
	}
}
