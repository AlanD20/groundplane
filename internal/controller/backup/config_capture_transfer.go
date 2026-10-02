package backup

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// OpenBackupConfigCapture selects only the durable snapshot named by this
// live assignment. It never re-evaluates Entry values or reads desired HEAD.
func (service *BackupCheckpointService) OpenBackupConfigCapture(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	authority *agentpb.BackupTaskAuthority,
	stepID string,
) (*CapturedConfigSnapshot, error) {
	if ctx == nil || service == nil || service.configSnapshots == nil {
		return nil, errs.New(errs.KindInternal, "Config transfer producer is unavailable")
	}
	digest, err := executionplan.BackupTaskAuthorityDigest(authority)
	if err != nil {
		return nil, err
	}
	claim, err := service.repository.GetBackupCheckpointAssignment(ctx, authority.TaskId)
	if err != nil {
		return nil, err
	}
	assignment := claim.Record
	if assignment.AgentID != agentID || assignment.AgentGeneration != agentGeneration ||
		assignment.AssignmentID != authority.AssignmentId || assignment.BackupAuthorityFence == nil ||
		assignment.BackupAuthorityFence.AssignmentGeneration != authority.AssignmentGeneration ||
		assignment.BackupAuthorityFence.AuthoritySHA256 != hex.EncodeToString(digest) {
		return nil, errs.New(errs.KindStateConflict, "Config transfer assignment authority changed")
	}
	run, err := service.repository.GetBackupRun(ctx, authority.TaskId)
	if err != nil {
		return nil, err
	}
	if run.Record.TaskID != authority.TaskId || run.Record.State != backupruntime.BackupRunRunning ||
		len(run.Record.Sources) != len(authority.Steps) {
		return nil, errs.New(errs.KindStateConflict, "Config transfer source authority changed")
	}
	for index, step := range authority.Steps {
		if step.StepId != stepID {
			continue
		}
		expected := step.GetCapture().GetConfig()
		source := run.Record.Sources[index]
		if expected == nil || source.Kind != backupruntime.BackupRuntimeSourceConfig || source.Snapshot.Config == nil ||
			expected.EnvironmentId != run.Record.EnvironmentID ||
			!assignment.BackupAuthorityFence.MatchesCheckpoint(authority.AssignmentGeneration,
				step.StepId, step.ExecutionId, hex.EncodeToString(step.StepDigest)) {
			return nil, errs.New(errs.KindStateConflict, "Config transfer is outside its sealed source")
		}
		return service.configSnapshots.OpenCapturedConfigSnapshot(ctx, backupplanning.BackupConfigSnapshotInput{
			TaskID: authority.TaskId, SnapshotID: source.Snapshot.Config.ConfigSnapshotID,
			EnvironmentID: run.Record.EnvironmentID, SourceID: source.SourceID,
			ReadRevision: source.Snapshot.Config.ReadRevision,
		}, expected)
	}
	return nil, errs.New(errs.KindValidationFailed, "Config transfer step is not part of this assignment")
}
