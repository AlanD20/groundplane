package backupconfigtransfer

import (
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// InitialCredit seals the metadata-only grant at the transfer's empty cursor.
// The receiver must persist it before delivery; creating a seal is not a write.
func InitialCredit(binding Binding) (*agentpb.BackupConfigCredit, error) {
	chain, err := InitialChainSHA256(binding)
	if err != nil {
		return nil, err
	}
	return SealCredit(binding, &agentpb.BackupConfigCredit{
		TaskId: binding.TaskID, AssignmentId: binding.AssignmentID,
		StepId: binding.StepID, ExecutionId: binding.ExecutionID, TransferId: binding.TransferID,
		Direction: binding.Direction, CreditSequence: 1, NextOrdinal: 1, CumulativeChainSha256: chain,
		Credit: &agentpb.BackupConfigCredit_MetadataCredit{MetadataCredit: &agentpb.BackupConfigMetadataCredit{
			RecordCredit: MetadataCreditRecords, ByteCredit: MetadataCreditBytes,
		}},
	})
}

// NextCredit returns a grant only at a bounded window boundary, complete
// metadata, restored EntryEnd, or End. It replenishes exactly consumed credit, not a
// whole additional window. The owner must sync the stage and descriptors,
// persist the returned credit, then install it with AcceptPersistedCredit.
func (receiver *Receiver) NextCredit() (*agentpb.BackupConfigCredit, error) {
	if receiver == nil || receiver.failed {
		return nil, invalid("Config receiver is unavailable")
	}
	window := receiver.window
	status := window.Status()
	if status.UnacknowledgedRecordCount == 0 {
		return nil, nil
	}
	credit := &agentpb.BackupConfigCredit{
		TaskId: receiver.binding.TaskID, AssignmentId: receiver.binding.AssignmentID,
		StepId: receiver.binding.StepID, ExecutionId: receiver.binding.ExecutionID,
		TransferId: receiver.binding.TransferID, Direction: receiver.binding.Direction,
		CreditSequence:          status.LastCreditSequence + 1,
		CommittedRecordSequence: receiver.progress.Cursor.RecordSequence,
		NextOrdinal:             receiver.progress.Cursor.NextOrdinal,
		CumulativeChainSha256:   append([]byte(nil), receiver.progress.Cursor.ChainSHA256[:]...),
	}
	if !status.MetadataAccepted {
		if receiver.progress.MetadataReady {
			credit.Credit = &agentpb.BackupConfigCredit_MetadataAccepted{
				MetadataAccepted: &agentpb.BackupConfigMetadataAccepted{
					MetadataTranscriptSha256: append([]byte(nil), receiver.progress.MetadataTranscriptSHA256[:]...),
					InitialValueCreditBytes:  ValueCreditBytes, InitialValueRecordCredit: ValueCreditRecords,
				},
			}
		} else {
			if status.MetadataRecordCredit != 0 {
				return nil, nil
			}
			credit.Credit = &agentpb.BackupConfigCredit_MetadataCredit{MetadataCredit: &agentpb.BackupConfigMetadataCredit{
				RecordCredit: MetadataCreditRecords, ByteCredit: MetadataCreditBytes,
			}}
		}
	} else {
		entryComplete := receiver.binding.Direction == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE &&
			receiver.progress.Cursor.NextOrdinal > status.Committed.NextOrdinal
		if status.ValueRecordCredit != 0 && !receiver.progress.Complete && !entryComplete {
			return nil, nil
		}
		var consumedBytes uint64
		var consumedRecords uint32
		for _, record := range window.pending {
			if record.lane != valueLane {
				return nil, invalid("Config value phase retains metadata records")
			}
			consumedBytes += record.bytes
			consumedRecords++
		}
		credit.Credit = &agentpb.BackupConfigCredit_ValueCredit{ValueCredit: &agentpb.BackupConfigValueCredit{
			ValueCreditBytes: consumedBytes, RecordCredit: consumedRecords,
		}}
	}
	return SealCredit(receiver.binding, credit)
}
