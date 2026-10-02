package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateBackupTaskResult(ack *agentpb.TaskAck) error {
	result := ack.GetBackupResult()
	if result == nil || result.RecoveryRequired == nil || executionplan.RejectUnknown(ack) != nil ||
		ack.AssignmentGeneration == 0 ||
		len(ack.ReleaseRecoveryRecordSha256) != 0 {
		return errs.New(errs.KindValidationFailed, "Agent Backup Task result is invalid")
	}
	if ack.Terminal == agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED &&
		(ack.ExitCode != 0 || result.FailedStepId != "" || result.GetRecoveryRequired()) {
		return errs.New(errs.KindValidationFailed, "completed Agent Backup Task result is inconsistent")
	}
	return nil
}

func durableBackupTaskResult(ack *agentpb.TaskAck) taskjournal.TaskResultRecord {
	return taskjournal.TaskResultRecord{
		Kind: taskjournal.TaskResultBackup, ExitCode: ack.ExitCode,
		FailedStepID:           ack.GetBackupResult().GetFailedStepId(),
		Diagnostic:             taskjournal.TaskResultDiagnosticNone,
		ReconciliationRequired: ack.GetBackupResult().GetRecoveryRequired(),
		ExecutionEpoch:         ack.ExecutionEpoch, AssignmentGeneration: ack.AssignmentGeneration,
	}
}
