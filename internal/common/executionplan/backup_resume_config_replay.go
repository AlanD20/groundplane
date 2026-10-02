package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func replayBackupConfigProgress(
	authority *agentpb.BackupTaskAuthority,
	step *agentpb.BackupStepAuthority,
	checkpoint *agentpb.BackupConfigCheckpoint,
	evidence *BackupConfigTransferEvidence,
	previous *agentpb.BackupConfigProgress,
	priorRecords uint64,
	transfer *agentpb.BackupConfigTransferCompleted,
	content *agentpb.BackupConfigContentAuthority,
	restore bool,
) (*agentpb.BackupConfigProgress, uint64, *agentpb.BackupConfigTransferCompleted, error) {
	if evidence == nil || evidence.MetadataAccepted == nil {
		return nil, 0, nil, errs.New(
			errs.KindNotImplemented,
			"backup Config resume requires durable metadata and committed transfer credits",
		)
	}
	metadata := evidence.MetadataAccepted
	if !validBackupConfigResumeCredit(metadata, authority, step, content, restore) ||
		metadata.GetMetadataAccepted() == nil ||
		!backupCheckpointDigest(metadata.GetMetadataAccepted().MetadataTranscriptSha256) {
		return nil, 0, nil, backupResumeHistoryInvalid()
	}
	credits := []*agentpb.BackupConfigCredit{metadata}
	for _, credit := range evidence.CommittedCredits {
		prior := credits[len(credits)-1]
		if !validBackupConfigResumeCredit(credit, authority, step, content, restore) ||
			credit.GetValueCredit() == nil ||
			credit.TransferId != metadata.TransferId ||
			credit.CreditSequence <= prior.CreditSequence ||
			credit.CommittedRecordSequence <= prior.CommittedRecordSequence ||
			credit.NextOrdinal < prior.NextOrdinal {
			return nil, 0, nil, backupResumeHistoryInvalid()
		}
		credits = append(credits, credit)
	}
	accepted := true
	progress := &agentpb.BackupConfigProgress{
		MetadataAccepted:         &accepted,
		MetadataTranscriptSha256: append([]byte(nil), metadata.GetMetadataAccepted().MetadataTranscriptSha256...),
	}
	if previous != nil {
		if previous.GetMaterialization() != nil ||
			!bytes.Equal(previous.MetadataTranscriptSha256, progress.MetadataTranscriptSha256) {
			return nil, 0, nil, backupResumeHistoryInvalid()
		}
		progress.NextValueOrdinal = previous.NextValueOrdinal
		progress.ValueChainSha256 = append([]byte(nil), previous.ValueChainSha256...)
	}
	var records uint64
	switch state := checkpoint.Checkpoint.(type) {
	case *agentpb.BackupConfigCheckpoint_ValueProgress:
		if state == nil || state.ValueProgress == nil || transfer != nil {
			return nil, 0, nil, backupResumeHistoryInvalid()
		}
		progress.NextValueOrdinal = state.ValueProgress.NextOrdinal
		progress.ValueChainSha256 = append([]byte(nil), state.ValueProgress.ChainSha256...)
	case *agentpb.BackupConfigCheckpoint_TransferCompleted:
		if state == nil || !validBackupConfigTransferCompleted(state.TransferCompleted) || transfer != nil || !proto.Equal(state.TransferCompleted.Content, content) {
			return nil, 0, nil, backupResumeHistoryInvalid()
		}
		transfer = state.TransferCompleted
		progress.NextValueOrdinal = content.EntryCount + 1
		progress.ValueChainSha256 = append([]byte(nil), transfer.ValueChainSha256...)
		progress.Progress = &agentpb.BackupConfigProgress_Transfer{Transfer: transfer}
	case *agentpb.BackupConfigCheckpoint_MaterializationVerified:
		if state == nil || !restore || transfer == nil || state.MaterializationVerified == nil ||
			state.MaterializationVerified.RestoreGenerationId != transfer.RestoreGenerationId || state.MaterializationVerified.MaterializedEntryCount != content.EntryCount {
			return nil, 0, nil, backupResumeHistoryInvalid()
		}
		progress.Progress = &agentpb.BackupConfigProgress_Materialization{Materialization: state.MaterializationVerified}
		records = priorRecords
	default:
		return nil, 0, nil, backupResumeHistoryInvalid()
	}
	if records == 0 {
		for _, credit := range credits {
			if credit.NextOrdinal == progress.NextValueOrdinal &&
				bytes.Equal(credit.CumulativeChainSha256, progress.ValueChainSha256) {
				records = credit.CommittedRecordSequence
			}
		}
	}
	if records == 0 || records < priorRecords ||
		(previous != nil && progress.NextValueOrdinal < previous.NextValueOrdinal) ||
		(progress.GetTransfer() != nil && records != transfer.CommittedRecordCount) ||
		!validBackupConfigResumeProgress(progress, content, restore) {
		return nil, 0, nil, backupResumeHistoryInvalid()
	}
	return progress, records, transfer, nil
}

func validBackupConfigResumeCredit(credit *agentpb.BackupConfigCredit, authority *agentpb.BackupTaskAuthority,
	step *agentpb.BackupStepAuthority, content *agentpb.BackupConfigContentAuthority, restore bool,
) bool {
	direction := agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE
	if restore {
		direction = agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE
	}
	return credit != nil && RejectUnknown(credit) == nil && credit.TaskId == authority.TaskId &&
		credit.AssignmentId == authority.AssignmentId &&
		credit.StepId == step.StepId &&
		credit.ExecutionId == step.ExecutionId &&
		validRawULID(credit.TransferId) &&
		credit.Direction == direction &&
		credit.CreditSequence > 0 &&
		credit.CommittedRecordSequence > 0 &&
		credit.NextOrdinal > 0 &&
		credit.NextOrdinal <= content.EntryCount+1 &&
		backupCheckpointDigest(credit.CumulativeChainSha256) &&
		backupCheckpointDigest(credit.AckSha256)
}
