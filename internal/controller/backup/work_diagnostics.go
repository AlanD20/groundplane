package backup

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Only committed source checkpoints become observations. One stable stage per
// source bounds large file/configuration streams. Details cannot grant effects.
func (owner *AgentWork) reportCheckpoint(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.BackupWorkInput, request *agentpb.BackupWorkCheckpointRequest,
	ack *agentpb.BackupWorkCheckpointAck,
) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	attempt := view.Attempt.Attempt
	snapshot, err := owner.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
	if err == nil && snapshot.Expired {
		return
	}
	if err == nil {
		at := time.Now().UTC()
		step := input.Steps[request.SourceOrdinal]
		action := "Back up"
		if view.Work.Item.Item.Operation == workexecution.Restore {
			action = "Restore"
		} else if view.Work.Item.Item.Operation == workexecution.BackupPrune {
			action = "Remove archive for"
		}
		stage := workexecution.Stage{Key: fmt.Sprintf("source_%d", request.SourceOrdinal),
			Name:     fmt.Sprintf("%s %s source %d", action, backupDiagnosticSource(step), request.SourceOrdinal+1),
			Expected: "Complete the captured source, verify its result and settle required cleanup. Timing starts at its first confirmed checkpoint.",
			State:    workexecution.StageRunning, StartedAt: &at,
			Observed: backupCheckpointObservation(request)}
		sequence := uint64(1)
		var previousStage *workexecution.Stage
		if snapshot.Record != nil {
			sequence = snapshot.Record.Sequence + 1
			for _, previous := range snapshot.Record.Stages {
				if previous.Key == stage.Key {
					if previous.FinishedAt != nil {
						return
					}
					stage.StartedAt = previous.StartedAt
					previousStage = &previous
				}
			}
		}
		if ack.GetSource().GetCapture().GetPhase() == agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_TERMINAL ||
			ack.GetSource().GetRestore().GetPhase() == agentpb.BackupRestorePhase_BACKUP_RESTORE_PHASE_TERMINAL ||
			ack.GetSource().GetPrune() != nil &&
				ack.Source.GetPrune().NextObjectOrdinal == uint32(len(step.GetPrune().GetObjects())) {
			stage.State, stage.FinishedAt = workexecution.StageSucceeded, &at
		}
		// Per-file/value checkpoints can arrive thousands of times. Keep the
		// live observation bounded without suppressing source completion.
		if previousStage != nil && stage.State == workexecution.StageRunning &&
			(stage.Observed == previousStage.Observed || at.Sub(snapshot.Record.UpdatedAt) < time.Second) {
			return
		}
		err = owner.diagnostics.Report(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID,
			attempt.Executor, sequence, stage, at)
	}
	if err != nil {
		slog.WarnContext(ctx, "Backup diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
		if markErr := owner.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); markErr != nil {
			slog.WarnContext(ctx, "could not record Backup diagnostic loss", "attempt_id", attempt.ID, "error", markErr)
		}
	}
}

// Source cleanup receipts can precede consumer recovery and final settlement.
// Only the committed attempt result closes the remaining source observations.
// No prior diagnostic record means no historical stage is reconstructed.
func (owner *AgentWork) reportTerminal(ctx context.Context, attempt workexecution.Attempt) {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	snapshot, err := owner.diagnostics.Read(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, 0)
	if err == nil && !snapshot.Expired && snapshot.Record != nil {
		sequence := snapshot.Record.Sequence
		for _, stage := range snapshot.Record.Stages {
			if stage.State != workexecution.StageRunning {
				continue
			}
			if attempt.Outcome == workexecution.Succeeded {
				stage.State, stage.FinishedAt = workexecution.StageSucceeded, attempt.FinishedAt
				stage.Observed = "Source result and required cleanup confirmed by the committed operation outcome"
			} else {
				stage.State = workexecution.StageUnknown
				stage.Observed = "The attempt stopped before source completion was confirmed; inspect its recorded outcome and cleanup state"
			}
			sequence++
			err = owner.diagnostics.ReportResult(bounded, attempt, sequence, stage)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		slog.WarnContext(ctx, "Backup outcome diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
		if markErr := owner.diagnostics.MarkIncomplete(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor); markErr != nil {
			slog.WarnContext(
				ctx,
				"could not record Backup outcome diagnostic loss",
				"attempt_id",
				attempt.ID,
				"error",
				markErr,
			)
		}
	}
}

func backupDiagnosticSource(step *agentpb.BackupStepAuthority) string {
	capture, restore := step.GetCapture(), step.GetRestore()
	switch {
	case capture.GetVolume() != nil || restore.GetVolume() != nil:
		return "Volume"
	case capture.GetConfig() != nil || restore.GetConfig() != nil:
		return "configuration"
	case capture.GetPostgres() != nil || restore.GetPostgres() != nil:
		return "PostgreSQL"
	case capture.GetMysql() != nil || restore.GetMysql() != nil:
		return "MySQL"
	default:
		return "captured"
	}
}

func backupCheckpointObservation(request *agentpb.BackupWorkCheckpointRequest) string {
	switch {
	case request.GetArtifactPrepared() != nil:
		return "Backup archive prepared; encrypted upload is next"
	case request.GetUploadCompleted() != nil:
		return "Upload completed; independent object verification is next"
	case request.GetUploadVerified() != nil:
		return "Uploaded archive verified against its captured byte evidence"
	case request.GetRestoreArtifactValidated() != nil:
		return "Downloaded recovery archive validated before target mutation"
	case request.GetSourceCleanupCompleted() != nil:
		return "Source staging cleanup confirmed"
	case request.GetPruneObjectDeleted() != nil:
		return "Exact captured remote object absence confirmed"
	case request.GetPostgresContainerObserved() != nil || request.GetMysqlContainerObserved() != nil:
		return "Database container identity and version confirmed"
	case request.GetPostgresDumpStart() != nil || request.GetMysqlDumpStart() != nil:
		return "Captured database dump authorized; its outcome is not yet confirmed"
	case request.GetPostgresRestoreApplyStart() != nil || request.GetMysqlRestoreApplyStart() != nil:
		return "Database Restore authorized; its outcome is not yet confirmed"
	case request.GetPostgresRestoreVerified() != nil || request.GetMysqlRestoreVerified() != nil:
		return "Database Restore result verified"
	case request.GetPostgresServiceProgress() != nil || request.GetMysqlServiceProgress() != nil || request.GetDatabaseConsumerRecovery() != nil:
		return "Captured database consumer recovery advanced"
	case request.GetConfig() != nil:
		return backupConfigObservation(request.GetConfig())
	case request.GetVolume() != nil:
		return backupVolumeObservation(request.GetVolume())
	default:
		return "Captured source checkpoint committed"
	}
}

func backupConfigObservation(checkpoint *agentpb.BackupConfigCheckpoint) string {
	switch {
	case checkpoint.GetTransferCompleted() != nil:
		return "Captured configuration transfer verified"
	case checkpoint.GetMaterializationVerified() != nil:
		return "Restored managed configuration files verified"
	default:
		return fmt.Sprintf(
			"Captured configuration values transferred: %d",
			checkpoint.GetValueProgress().GetNextOrdinal(),
		)
	}
}

func backupVolumeObservation(checkpoint *agentpb.BackupVolumeCheckpoint) string {
	switch {
	case checkpoint.GetConsumersStopped() != nil:
		return "Captured Volume consumers stopped before replacement"
	case checkpoint.GetConstructionIntent() != nil:
		return "Replacement tree construction authorized"
	case checkpoint.GetConstructionCompleted() != nil:
		return "Replacement tree construction progress verified"
	case checkpoint.GetFinalizationIntent() != nil:
		return "Replacement tree permissions and ownership finalization authorized"
	case checkpoint.GetFinalizationCompleted() != nil:
		return "Replacement tree permissions and ownership progress verified"
	case checkpoint.GetExchangeIntent() != nil:
		return "Replacement tree exchange authorized; completion is not yet confirmed"
	case checkpoint.GetTreeExchanged() != nil:
		return "Restored tree exchanged into the captured Volume"
	case checkpoint.GetReplacedPathDeleteIntent() != nil:
		return "Replaced tree cleanup authorized"
	case checkpoint.GetVolumeReplacedPathCleaned() != nil:
		return "Replaced tree cleanup confirmed"
	default:
		return "Captured Volume consumer recovery advanced"
	}
}
