package backupsecrets

import (
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (reader *Reader) decodeSourceDynamicEvidence(
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	evidence *Evidence,
	plan *agentpb.ExecutionPlan,
	stepIndex int,
	sourceIDs map[string]int,
) error {
	if evidence.Restore != nil {
		return decodeConfigRestoreSourceEvidence(result, dynamic, evidence)
	}
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
	step := plan.Steps[stepIndex].GetBackupStep().GetCapture()
	if step == nil {
		return errs.New(errs.KindStateConflict, "backup source step evidence changed")
	}
	if err := reader.validateCaptureTargetEvidence(result, dynamic, evidence.Run, source, step); err != nil {
		return err
	}
	return nil
}

func decodeConfigRestoreSourceEvidence(result *etcdstore.GetManyResult, dynamic *backupSecretDynamicRead,
	evidence *Evidence,
) error {
	restored := evidence.Restore
	sourceValue := result.Values[dynamic.sources[restored.Point.SourceID]]
	pointValue := result.Values[dynamic.points[restored.Point.ID]]
	if sourceValue == nil || sourceValue.ModRevision != restored.SourceRevision ||
		pointValue == nil || pointValue.ModRevision != restored.RecoveryPointRevision {
		return errs.New(errs.KindStateConflict, "Config Restore source or Recovery Point changed")
	}
	source, err := backuppolicy.DecodeBackupSourceRecord(sourceValue.Value)
	if err != nil || source.ID != restored.Point.SourceID || source.EnvironmentID != restored.EnvironmentID ||
		string(source.Kind) != string(restored.Point.SourceKind) || source.TargetID != restored.Point.TargetID {
		return errs.New(errs.KindStateConflict, "Config Restore source identity changed")
	}
	point, err := backupruntime.DecodeBackupRecoveryPointRecord(pointValue.Value)
	if err != nil || point.BackupRecoveryPointSnapshot != restored.Point {
		return errs.New(errs.KindStateConflict, "Config Restore selected Recovery Point changed")
	}
	evidence.Source, evidence.SourceRevision, evidence.Point = source, sourceValue.ModRevision, &point
	return nil
}

func (reader *Reader) validateCaptureTargetEvidence(
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	run *backupruntime.BackupRunRecord,
	source backupruntime.BackupRunSourceAttemptRecord,
	step *agentpb.BackupCaptureAuthority,
) error {
	switch source.Kind {
	case backupruntime.BackupRuntimeSourceAttach:
		snapshot := source.Snapshot.Postgres
		if snapshot == nil {
			return errs.New(errs.KindInternal, "postgres backup source snapshot is corrupt")
		}
		keys := []*etcdstore.KeyValue{
			result.Values[dynamic.index[attachrecord.AttachKey(source.TargetID)]],
			result.Values[dynamic.index[attachrecord.AttachFactsKey(source.TargetID)]],
			result.Values[dynamic.index[hierarchyrecord.ProjectKey(snapshot.BackingProjectID)]],
			result.Values[dynamic.index[hierarchyrecord.EnvironmentKey(snapshot.BackingEnvironmentID)]],
			result.Values[dynamic.index[blueprints.EnvironmentBlueprintHeadKey(snapshot.BackingEnvironmentID)]],
		}
		if !backupSecretRecordMatches(keys[0], step.GetResource().GetResource()) {
			return errs.New(errs.KindStateConflict, "postgres backup sealed resource changed")
		}
		return backupplanning.ValidateBackupPostgresPublicationEvidence(keys, source, *snapshot)
	case backupruntime.BackupRuntimeSourceVolume:
		snapshot := source.Snapshot.Volume
		if snapshot == nil {
			return errs.New(errs.KindInternal, "volume backup source snapshot is corrupt")
		}
		keys := make([]*etcdstore.KeyValue, 0, len(snapshot.Services)+3)
		keys = append(
			keys,
			result.Values[dynamic.index[hierarchyrecord.EnvironmentKey(snapshot.EnvironmentID)]],
			result.Values[dynamic.index[blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID)]],
			result.Values[dynamic.index[blueprints.EnvironmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID)]],
		)
		for _, service := range snapshot.Services {
			keys = append(keys, result.Values[dynamic.index[servicerecord.ServiceRuntimeKey(service.ServiceID)]])
		}
		if !backupSecretRecordMatches(keys[1], step.GetResource().GetResource()) {
			return errs.New(errs.KindStateConflict, "volume backup sealed resource changed")
		}
		return backupplanning.ValidateBackupVolumePublicationEvidence(keys, source, *snapshot)
	case backupruntime.BackupRuntimeSourceConfig:
		snapshot := source.Snapshot.Config
		config := step.GetConfig()
		if snapshot == nil || config == nil || snapshot.ConfigSnapshotID != run.TaskID ||
			config.MetadataSnapshotRevision != snapshot.ReadRevision {
			return errs.New(errs.KindStateConflict, "backup config snapshot revision changed")
		}
		target := result.Values[dynamic.index[hierarchyrecord.EnvironmentKey(run.EnvironmentID)]]
		if target == nil || target.ModRevision != source.TargetRevision ||
			!backupSecretRecordMatches(target, step.GetResource().GetResource()) {
			return errs.New(errs.KindStateConflict, "backup config target evidence changed")
		}
		return nil
	default:
		return errs.New(errs.KindInternal, "backup source kind is corrupt")
	}
}
