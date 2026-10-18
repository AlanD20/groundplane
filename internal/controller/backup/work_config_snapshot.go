package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Complete the captured configuration snapshot before handing capture work to
// the Agent. Re-delivery resumes the same append cursor, never a newer selection.
func (owner *AgentWork) captureWorkConfig(ctx context.Context, view workqueue.ResumeView,
	assignment *agentpb.BackupWorkAssignment,
) error {
	for ordinal, step := range assignment.Input.Steps {
		expected := step.GetCapture().GetConfig()
		if expected == nil {
			continue
		}
		run, _, err := owner.inputs.LoadStagingCapture(ctx, backupwork.StagingSource{
			View: view.Work, Input: assignment.Input, Ordinal: uint32(ordinal),
		})
		if err != nil {
			return err
		}
		source := run.Sources[ordinal]
		if source.Kind != backupruntime.BackupRuntimeSourceConfig || source.Snapshot.Config == nil {
			return configSnapshotGuardConflict()
		}
		input := backupplanning.BackupConfigSnapshotInput{TaskID: run.TaskID,
			SnapshotID: source.Snapshot.Config.ConfigSnapshotID, EnvironmentID: run.EnvironmentID,
			SourceID: source.SourceID, ReadRevision: source.Snapshot.Config.ReadRevision}
		deadline := time.Unix(0, int64(step.StepDeadlineUnixNano))
		if view.Attempt.Attempt.Deadline.Before(deadline) {
			deadline = view.Attempt.Attempt.Deadline
		}
		captureCtx, cancel := context.WithDeadline(ctx, deadline)
		err = owner.configSnapshots.CapturePublished(captureCtx, input, expected)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}
