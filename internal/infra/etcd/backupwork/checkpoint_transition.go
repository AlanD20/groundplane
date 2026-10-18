package backupwork

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumemanifest"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CheckpointEvidence comes from durable transfer/manifest owners at the
// assignment read revision, never from the Agent report. Conditions fence that
// evidence in the same commit as progress and operation-specific settlement.
type CheckpointEvidence struct {
	Config          *executionplan.BackupConfigWorkEvidence
	VolumeManifest  *agentpb.BackupVolumeArchiveEvidence
	VolumeReference backupruntime.BackupVolumeManifestReference
	Conditions      []keyvalue.Condition
}

func advanceCheckpoint(admitted CheckpointAdmission, executor workexecution.Identity,
	evidence CheckpointEvidence, readRevision int64,
) (*agentpb.BackupStepResume, error) {
	assignment, report := admitted.Assignment, admitted.Request
	step := assignment.Input.Steps[report.SourceOrdinal]
	if evidence.Config != nil || evidence.VolumeManifest != nil {
		if len(evidence.Conditions) == 0 {
			return nil, conflict("Backup transfer evidence has no durable owner guard")
		}
		for _, guard := range evidence.Conditions {
			if guard.Key == "" || guard.ModRevision <= 0 || guard.ModRevision > readRevision {
				return nil, conflict("Backup transfer evidence is outside the captured snapshot")
			}
		}
	}
	if evidence.Config != nil {
		matched := false
		for _, guard := range evidence.Conditions {
			matched = matched || guard.ModRevision == evidence.Config.Revision
		}
		if !matched {
			return nil, conflict("Backup Config transfer revision is not fenced")
		}
	}
	if evidence.VolumeManifest != nil {
		ref := evidence.VolumeReference
		if !ref.Valid() || ref.Binding.Attempt.ID != assignment.Attempt.AttemptId ||
			ref.Binding.SourceOrdinal != report.SourceOrdinal {
			return nil, conflict("Volume manifest belongs to different native work")
		}
		matched := false
		for _, guard := range evidence.Conditions {
			matched = matched || guard.Key == backupvolumemanifest.CursorKey(ref.Binding) && guard.ModRevision == ref.CursorRevision
		}
		if !matched {
			return nil, conflict("Volume manifest cursor is not fenced")
		}
	}
	switch assignment.Input.Operation {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		switch {
		case step.GetCapture().GetPostgres() != nil, step.GetCapture().GetMysql() != nil:
			return executionplan.AdvanceBackupDatabaseCapture(assignment, report, executor)
		case step.GetCapture().GetVolume() != nil:
			return executionplan.AdvanceBackupVolumeCapture(assignment, report, executor, evidence.VolumeManifest)
		case step.GetCapture().GetConfig() != nil:
			return executionplan.AdvanceBackupConfigCapture(assignment, report, executor, evidence.Config)
		}
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		switch {
		case step.GetRestore().GetPostgres() != nil, step.GetRestore().GetMysql() != nil:
			return executionplan.AdvanceBackupDatabaseRestore(assignment, report, executor)
		case step.GetRestore().GetVolume() != nil:
			return executionplan.AdvanceBackupVolumeRestore(assignment, report, executor)
		case step.GetRestore().GetConfig() != nil:
			return executionplan.AdvanceBackupConfigRestore(assignment, report, executor, evidence.Config)
		}
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE:
		return executionplan.AdvanceBackupPrune(assignment, report, executor)
	}
	return nil, conflict("Backup checkpoint has an unsupported source operation")
}

func checkpointGrantsEffect(report *agentpb.BackupWorkCheckpointRequest) bool {
	volume := report.GetVolume()
	return report.GetPostgresDumpStart() != nil || report.GetMysqlDumpStart() != nil || report.GetArtifactPrepared() != nil ||
		report.GetPostgresRestoreApplyStart() != nil || report.GetMysqlRestoreApplyStart() != nil ||
		report.GetPostgresServiceProgress().GetPhase() == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT ||
		report.GetMysqlServiceProgress().GetPhase() == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT ||
		volume.GetServiceProgress().GetPhase() == agentpb.BackupServicePhase_BACKUP_SERVICE_PHASE_STOP_INTENT ||
		volume.GetConstructionIntent() != nil || volume.GetFinalizationIntent() != nil || volume.GetExchangeIntent() != nil
}
