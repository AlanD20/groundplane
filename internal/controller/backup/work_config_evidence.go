package backup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Capture evidence is reconstructed from durable credits and the pinned
// snapshot. An Agent-supplied transcript cannot authorize Point publication.
func (owner *AgentWork) configCaptureEvidence(ctx context.Context, view workqueue.ResumeView,
	admitted backupwork.CheckpointAdmission,
) (backupwork.CheckpointEvidence, error) {
	binding, err := backupconfigtransfer.Bind(admitted.Assignment, view.Attempt.Attempt.Executor,
		admitted.Request.SourceOrdinal, agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE)
	if err != nil {
		return backupwork.CheckpointEvidence{}, err
	}
	cursor, found, err := owner.configTransfers.ReadConfigTransferCursor(ctx, binding, view.Work.ReadRevision)
	if err != nil {
		return backupwork.CheckpointEvidence{}, err
	}
	if !found || cursor.Record.Owner.Binding != binding || cursor.Record.MetadataAcceptedCreditSequence == 0 {
		return backupwork.CheckpointEvidence{}, invalidConfigCaptureCompletion()
	}
	var credits []*agentpb.BackupConfigCredit
	var metadata *agentpb.BackupConfigMetadataAccepted
	err = owner.configTransfers.VisitConfigTransferCredits(ctx, cursor, func(credit *agentpb.BackupConfigCredit) error {
		credits = append(credits, credit)
		if credit.GetMetadataAccepted() != nil {
			metadata = credit.GetMetadataAccepted()
		}
		return nil
	})
	if err != nil {
		return backupwork.CheckpointEvidence{}, err
	}
	if len(credits) == 0 || metadata == nil {
		return backupwork.CheckpointEvidence{}, invalidConfigCaptureCompletion()
	}
	final := credits[len(credits)-1]
	if final.CreditSequence != cursor.Record.LastCreditSequence {
		return backupwork.CheckpointEvidence{}, invalidConfigCaptureCompletion()
	}
	attempt := binding.Attempt
	evidence := &executionplan.BackupConfigWorkEvidence{
		TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, Input: attempt.Input,
		SourceOrdinal: binding.SourceOrdinal, Revision: cursor.Revision,
		MetadataTranscriptSHA256: append([]byte(nil), metadata.MetadataTranscriptSha256...),
		CommittedRecords:         final.CommittedRecordSequence, NextOrdinal: final.NextOrdinal,
		ValueChainSHA256: append([]byte(nil), final.CumulativeChainSha256...),
	}
	if admitted.Request.GetConfig().GetTransferCompleted() != nil {
		step := admitted.Assignment.Input.Steps[binding.SourceOrdinal]
		content := step.GetCapture().GetConfig().GetContent()
		if final.GetValueCredit() == nil || final.NextOrdinal != content.EntryCount+1 {
			return backupwork.CheckpointEvidence{}, invalidConfigCaptureCompletion()
		}
		run, _, err := owner.inputs.LoadStagingCapture(ctx, backupwork.StagingSource{
			View: view.Work, Input: admitted.Assignment.Input, Sources: admitted.Assignment.Sources,
			Ordinal: binding.SourceOrdinal,
		})
		if err != nil {
			return backupwork.CheckpointEvidence{}, err
		}
		source := run.Sources[binding.SourceOrdinal]
		if source.Snapshot.Config == nil {
			return backupwork.CheckpointEvidence{}, invalidConfigCaptureCompletion()
		}
		snapshot, err := owner.configSnapshots.OpenCapturedConfigSnapshot(ctx, backupplanning.BackupConfigSnapshotInput{
			TaskID: run.TaskID, SnapshotID: source.Snapshot.Config.ConfigSnapshotID,
			EnvironmentID: run.EnvironmentID, SourceID: source.SourceID,
			ReadRevision: source.Snapshot.Config.ReadRevision,
		}, step.GetCapture().GetConfig())
		if err != nil {
			return backupwork.CheckpointEvidence{}, err
		}
		transcript, err := snapshot.StreamCapture(ctx, binding, configCaptureProofTransport{}, credits, final)
		if err != nil {
			return backupwork.CheckpointEvidence{}, err
		}
		evidence.TransferTranscriptSHA256 = append([]byte(nil), transcript[:]...)
	}
	return backupwork.CheckpointEvidence{Config: evidence, Conditions: []keyvalue.Condition{{
		Key: backupconfiguration.ConfigTransferCursorKey(binding), ModRevision: cursor.Revision,
	}}}, nil
}
