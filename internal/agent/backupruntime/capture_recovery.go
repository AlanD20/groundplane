package backupruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func recoverCapture(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	attempt workexecution.Attempt, source executionplan.BackupWorkRecovery,
	ownership *agentownership.Lease, staging *Staging,
) (executionplan.BackupRecoveryReport, error) {
	var zero executionplan.BackupRecoveryReport
	if staging == nil || ownership == nil || attempt.StartedAt == nil ||
		assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE {
		return zero, errs.New(errs.KindStateConflict, "Backup cleanup requires captured sources and staging ownership")
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return zero, err
	}
	report := executionplan.BackupRecoveryReport{AttemptID: attempt.ID, ForwardID: source.Forward.ID,
		Failure: source.Evidence, ExecutorStopped: true, Operation: workexecution.Backup}
	for ordinal, step := range assignment.Input.Steps {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		if err := retireSettledSourceHelpers(ctx, assignment.Input, step, false); err != nil {
			return zero, err
		}
		ids := backupstage.IDs{Task: attempt.TaskID, Step: step.ExecutionId, Point: step.GetCapture().GetPointId()}
		if err := staging.cleanupCapturedSource(ctx, ids); err != nil {
			return zero, err
		}
		report.Sources = append(report.Sources, executionplan.BackupSourceRecoveryProof{
			Ordinal: uint32(ordinal), PointID: ids.Point, StagingAbsent: true, HelperRetired: true,
		})
	}
	if err := ownership.CheckRecovery(source.Forward, attempt); err != nil {
		return zero, err
	}
	report.ObservedAt = time.Now().UTC()
	if err := report.ValidateInput(assignment.Input); err != nil {
		return zero, err
	}
	return report, report.Validate(attempt, source)
}

// Restore apply evidence may be retired only after acknowledged completion.
// Capture and pre-mutation Restore cleanup must reject any apply in the namespace.
func retireSettledSourceHelpers(
	ctx context.Context,
	input *agentpb.BackupWorkInput,
	step *agentpb.BackupStepAuthority,
	completedRestore bool,
) error {
	capture := step.GetCapture()
	restore := step.GetRestore()
	if capture == nil && restore == nil {
		return invalidDatabaseStaging()
	}
	serviceID := capture.GetPostgres().GetDatabaseServiceId()
	if serviceID == "" {
		serviceID = capture.GetMysql().GetDatabaseServiceId()
	}
	if serviceID == "" {
		serviceID = restore.GetPostgres().GetDatabaseServiceId()
	}
	if serviceID == "" {
		serviceID = restore.GetMysql().GetDatabaseServiceId()
	}
	if serviceID == "" {
		return nil // Config and Volume capture have no detached database helper.
	}
	guard := &agentpb.BackupDatabaseStagingGuard{Step: step, TerminalCleanup: true}
	for _, service := range input.Services {
		if service.ServiceId == serviceID {
			if guard.DatabaseService != nil {
				return invalidDatabaseStaging()
			}
			guard.DatabaseService = service
		}
	}
	if guard.DatabaseService == nil {
		return invalidDatabaseStaging()
	}
	for _, artifact := range input.Artifacts {
		for _, candidate := range artifact.GetServices() {
			if candidate.GetServiceId() != serviceID ||
				candidate.GetComposeName() != guard.DatabaseService.CurrentName {
				continue
			}
			if guard.DatabaseArtifact != nil {
				return invalidDatabaseStaging()
			}
			guard.DatabaseArtifact = artifact
		}
	}
	if guard.DatabaseArtifact == nil {
		return invalidDatabaseStaging()
	}
	return inspectDatabaseStaging(ctx, &agentpb.BackupStagingDisposition{DatabaseGuard: guard}, completedRestore)
}
