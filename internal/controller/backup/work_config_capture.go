package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// StreamConfigCapture transfers only the snapshot pinned by this work input.
// Credits must be durably committed before their acknowledgements are sent.
func (owner *AgentWork) StreamConfigCapture(ctx context.Context, authorization agentregistration.WorkAuthorization,
	binding backupconfigtransfer.Binding, receiverCredit *agentpb.BackupConfigCredit,
	transport backupconfigtransfer.StreamTransport,
) error {
	if _, err := authorization.Conditions(); err != nil {
		return err
	}
	if binding.Validate() != nil || binding.Attempt.Executor != authorization.Identity() ||
		binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE || transport == nil {
		return configSnapshotGuardConflict()
	}
	issued, err := backupconfigtransfer.ValidateCredit(binding, receiverCredit)
	if err != nil {
		return err
	}
	attempt := binding.Attempt
	view, err := authorization.ReadResult(ctx, owner.queue, attempt.TaskID, attempt.WorkItemID, attempt.ID)
	if err != nil {
		return err
	}
	assignment, _, err := owner.inputs.LoadConfigTransfer(ctx, owner.queue, binding, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	step := assignment.Input.Steps[binding.SourceOrdinal]
	ctx, cancel := context.WithDeadline(ctx, time.Unix(0, int64(step.StepDeadlineUnixNano)))
	defer cancel()
	run, _, err := owner.inputs.LoadStagingCapture(ctx, backupwork.StagingSource{
		View: view.Work, Input: assignment.Input, Sources: assignment.Sources, Ordinal: binding.SourceOrdinal})
	if err != nil {
		return err
	}
	source := run.Sources[binding.SourceOrdinal]
	if source.Snapshot.Config == nil {
		return configSnapshotGuardConflict()
	}
	snapshot, err := owner.configSnapshots.OpenCapturedConfigSnapshot(ctx, backupplanning.BackupConfigSnapshotInput{
		TaskID: run.TaskID, SnapshotID: source.Snapshot.Config.ConfigSnapshotID, EnvironmentID: run.EnvironmentID,
		SourceID: source.SourceID, ReadRevision: source.Snapshot.Config.ReadRevision,
	}, step.GetCapture().GetConfig())
	if err != nil {
		return err
	}
	cursor, found, err := owner.configTransfers.ReadConfigTransferCursor(ctx, binding, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	var retained []*agentpb.BackupConfigCredit
	if found {
		if cursor.Record.Owner.Binding != binding {
			return configSnapshotGuardConflict()
		}
		if err := owner.configTransfers.VisitConfigTransferCredits(ctx, cursor, func(credit *agentpb.BackupConfigCredit) error {
			retained = append(retained, credit)
			return nil
		}); err != nil {
			return err
		}
	} else {
		initial, err := backupconfigtransfer.InitialCredit(binding)
		if err != nil {
			return err
		}
		if !proto.Equal(initial, issued) {
			return configSnapshotGuardConflict()
		}
		if err := transport.CommitCredit(ctx, initial); err != nil {
			return err
		}
		retained = []*agentpb.BackupConfigCredit{initial}
	}
	if found && len(retained) > 0 && proto.Equal(retained[len(retained)-1], issued) {
		if err := transport.CommitCredit(ctx, issued); err != nil {
			return err
		}
	}
	_, err = snapshot.StreamCapture(ctx, binding, transport, retained, issued)
	return err
}

func (owner *AgentWork) CommitConfigCaptureCredit(ctx context.Context, authorization agentregistration.WorkAuthorization,
	binding backupconfigtransfer.Binding, credit *agentpb.BackupConfigCredit,
) error {
	if _, err := authorization.Conditions(); err != nil {
		return err
	}
	if binding.Validate() != nil || binding.Attempt.Executor != authorization.Identity() ||
		binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE {
		return configSnapshotGuardConflict()
	}
	_, err := owner.configTransfers.CommitConfigTransferCredit(ctx, backupconfiguration.ConfigTransferOwner{Binding: binding}, credit)
	return err
}
