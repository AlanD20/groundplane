package backup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Capture happens after atomic Task/snapshot publication and before assignment
// delivery. The writer fences the sealed procedure, active Task and Environment
// lock for each bounded append. Reconnect verifies or resumes this same snapshot.
func (service *BackupCheckpointService) captureAssignmentConfig(
	ctx context.Context,
	authority *agentpb.BackupTaskAuthority,
) error {
	needsConfig := false
	for _, step := range authority.Steps {
		if step.GetCapture().GetConfig() != nil {
			needsConfig = true
			break
		}
	}
	if !needsConfig {
		return nil
	}
	if service.configSnapshots == nil {
		return errs.New(errs.KindInternal, "Config snapshot producer is unavailable")
	}
	run, err := service.repository.GetBackupRun(ctx, authority.TaskId)
	if err != nil {
		return err
	}
	if run.Record.TaskID != authority.TaskId || len(run.Record.Sources) != len(authority.Steps) {
		return errs.New(errs.KindStateConflict, "Config snapshot run authority changed")
	}
	for index, step := range authority.Steps {
		expected := step.GetCapture().GetConfig()
		if expected == nil {
			continue
		}
		source := run.Record.Sources[index]
		if source.Kind != backupruntime.BackupRuntimeSourceConfig || source.Snapshot.Config == nil {
			return errs.New(errs.KindStateConflict, "Config snapshot source authority changed")
		}
		input := backupplanning.BackupConfigSnapshotInput{
			TaskID:        run.Record.TaskID,
			SnapshotID:    source.Snapshot.Config.ConfigSnapshotID,
			EnvironmentID: run.Record.EnvironmentID,
			SourceID:      source.SourceID,
			ReadRevision:  source.Snapshot.Config.ReadRevision,
		}
		if err := service.configSnapshots.CapturePublished(ctx, input, expected); err != nil {
			return err
		}
	}
	return nil
}
