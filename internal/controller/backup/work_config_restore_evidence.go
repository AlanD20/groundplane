package backup

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Restore evidence joins the protected complete-generation seal to the same
// transfer cursor snapshot as checkpoint admission. Neither an Agent digest
// nor an earlier accepted prefix proves that all selected values arrived.
func (owner *AgentWork) configRestoreEvidence(ctx context.Context, view workqueue.ResumeView,
	admitted backupwork.CheckpointAdmission,
) (backupwork.CheckpointEvidence, error) {
	binding, err := admitted.ConfigBinding(view.Attempt.Attempt.Executor)
	if err != nil {
		return backupwork.CheckpointEvidence{}, err
	}
	step := admitted.Input().Steps[binding.SourceOrdinal]
	transfer, err := configRestoreTransferOwner(backupconfiguration.ConfigTransferOwner{Binding: binding}, step.GetRestore())
	if err != nil {
		return backupwork.CheckpointEvidence{}, err
	}
	cursor, found, err := owner.configTransfers.ReadConfigTransferCursor(ctx, binding, view.Work.ReadRevision)
	if err != nil {
		return backupwork.CheckpointEvidence{}, err
	}
	if !found || cursor.Record.Owner != transfer.Transfer || cursor.Record.MetadataAcceptedCreditSequence == 0 {
		return backupwork.CheckpointEvidence{}, configSnapshotGuardConflict()
	}
	var metadata *agentpb.BackupConfigMetadataAccepted
	var final *agentpb.BackupConfigCredit
	err = owner.configTransfers.VisitConfigTransferCredits(ctx, cursor, func(credit *agentpb.BackupConfigCredit) error {
		if credit.GetMetadataAccepted() != nil {
			metadata = credit.GetMetadataAccepted()
		}
		final = credit
		return nil
	})
	if err != nil {
		return backupwork.CheckpointEvidence{}, err
	}
	if metadata == nil || final == nil || final.CreditSequence != cursor.Record.LastCreditSequence {
		return backupwork.CheckpointEvidence{}, configSnapshotGuardConflict()
	}
	attempt := binding.Attempt
	result := backupwork.CheckpointEvidence{
		Config: &executionplan.BackupConfigWorkEvidence{
			TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, Input: attempt.Input,
			SourceOrdinal: binding.SourceOrdinal, Revision: cursor.Revision,
			MetadataTranscriptSHA256: append([]byte(nil), metadata.MetadataTranscriptSha256...),
			CommittedRecords:         final.CommittedRecordSequence, NextOrdinal: final.NextOrdinal,
			ValueChainSHA256: append([]byte(nil), final.CumulativeChainSha256...),
		},
		Conditions: []keyvalue.Condition{{Key: backupconfiguration.ConfigTransferCursorKey(binding), ModRevision: cursor.Revision}},
	}
	if admitted.Request.GetConfig().GetValueProgress() != nil {
		return result, nil
	}
	seal, found, err := owner.configTransfers.ReadConfigRestoreGeneration(ctx, transfer, view.Work.ReadRevision)
	if err != nil {
		return backupwork.CheckpointEvidence{}, err
	}
	completed := seal.Record.Completed
	content := step.GetRestore().GetConfig().GetExpectedArchive().GetContent()
	if !found || completed == nil || final.GetValueCredit() == nil ||
		!proto.Equal(completed.Content, content) || completed.RestoreGenerationId != transfer.GenerationID ||
		completed.RenderGeneration != transfer.RenderGeneration ||
		completed.CommittedRecordCount != final.CommittedRecordSequence ||
		final.NextOrdinal != content.EntryCount+1 ||
		!bytes.Equal(completed.ValueChainSha256, final.CumulativeChainSha256) {
		return backupwork.CheckpointEvidence{}, configSnapshotGuardConflict()
	}
	result.Config.TransferTranscriptSHA256 = append([]byte(nil), completed.TransferTranscriptSha256...)
	result.Conditions = append(result.Conditions, keyvalue.Condition{
		Key: backupconfiguration.ConfigRestoreGenerationKey(transfer), ModRevision: seal.Revision,
	})
	return result, nil
}
