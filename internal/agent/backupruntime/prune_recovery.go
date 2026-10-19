package backupruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// InspectPruneRecovery runs under the recovery worker's local execution lease.
// Prune uses joined, in-process HTTP calls, not detached helpers or Docker jobs.
// The lease therefore excludes any former executor that could initiate another
// delete. It does not claim that the object store has undone accepted requests.
func InspectPruneRecovery(ctx context.Context, ownership *agentownership.Lease,
	attempt workexecution.Attempt, input *agentpb.BackupWorkInput, source executionplan.BackupWorkRecovery,
) (executionplan.BackupRecoveryReport, error) {
	if ctx == nil || ownership == nil || input == nil ||
		input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE ||
		attempt.StartedAt == nil ||
		attempt.FinishedAt != nil {
		return executionplan.BackupRecoveryReport{}, errs.New(
			errs.KindStateConflict,
			"Backup prune inspection requires its started recovery and host ownership",
		)
	}
	if err := ctx.Err(); err != nil {
		return executionplan.BackupRecoveryReport{}, err
	}
	if err := source.ValidateFor(attempt); err != nil {
		return executionplan.BackupRecoveryReport{}, err
	}
	_, raw, reference, err := executionplan.CaptureBackupWorkInput(attempt.Input.ID, input)
	clear(raw)
	if err != nil {
		return executionplan.BackupRecoveryReport{}, err
	}
	if reference != attempt.Input {
		return executionplan.BackupRecoveryReport{}, errs.New(
			errs.KindStateConflict,
			"Backup prune recovery input differs from its failed execution",
		)
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return executionplan.BackupRecoveryReport{}, err
	}
	report := executionplan.BackupRecoveryReport{AttemptID: attempt.ID, ForwardID: source.Forward.ID,
		Failure: source.Evidence, Operation: workexecution.BackupPrune, ExecutorStopped: true, ObservedAt: time.Now().UTC()}
	return report, report.Validate(attempt, source)
}
