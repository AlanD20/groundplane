package backupconfiguration

import (
	"bytes"
	"context"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"google.golang.org/protobuf/proto"
)

// SealConfigRestoreGeneration publishes only the completed protected journal.
// It compares the exact last credit and its final retained record, so future
// publication cannot mistake an earlier acknowledged prefix for a full set.
func (repository *ConfigTransferRepository) SealConfigRestoreGeneration(
	ctx context.Context,
	record ConfigRestoreGenerationRecord,
) (int64, error) {
	encoded, err := EncodeConfigRestoreGeneration(record)
	if err != nil {
		return 0, err
	}
	defer clear(encoded)
	binding := record.Owner.Transfer.Binding
	cursor, found, err := repository.ReadConfigTransferCursor(
		ctx,
		binding.TaskID,
		binding.AssignmentID,
		binding.StepID,
		binding.ExecutionID,
		0,
	)
	if err != nil {
		return 0, err
	}
	if !found || cursor.Record.Owner != record.Owner.Transfer || cursor.Record.MetadataAcceptedCreditSequence == 0 {
		return 0, captureSnapshotConflict()
	}
	conditions, err := repository.guard(ctx, record.Owner.Transfer, cursor.ReadRevision)
	if err != nil {
		return 0, err
	}
	more, err := repository.restoreGuard(ctx, record.Owner, cursor.ReadRevision)
	if err != nil {
		return 0, err
	}
	conditions = append(conditions, more...)
	keys := []string{
		ConfigRestoreGenerationKey(record.Owner),
		ConfigTransferCreditKey(binding, cursor.Record.LastCreditSequence),
		ConfigRestoreTransferRecordKey(binding, record.Completed.CommittedRecordCount),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: cursor.ReadRevision})
	if err != nil {
		return 0, err
	}
	if read == nil || read.ReadRevision != cursor.ReadRevision || len(read.Values) != len(keys) ||
		read.Values[1] == nil || read.Values[2] == nil {
		return 0, captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	for index := 1; index < len(read.Values); index++ {
		value := read.Values[index]
		if value.Key != keys[index] || value.Version != 1 || value.ModRevision <= 0 ||
			value.ModRevision > cursor.Revision {
			return 0, captureSnapshotConflict()
		}
		conditions = append(conditions, etcdstore.Condition{Key: keys[index], ModRevision: value.ModRevision})
	}
	credit, err := DecodeConfigTransferCredit(read.Values[1].Value)
	if err != nil || credit.Owner != record.Owner.Transfer ||
		credit.Credit.CreditSequence != cursor.Record.LastCreditSequence ||
		credit.Credit.GetValueCredit() == nil ||
		credit.Credit.CommittedRecordSequence != record.Completed.CommittedRecordCount ||
		credit.Credit.NextOrdinal != record.Completed.Content.EntryCount+1 ||
		!bytes.Equal(credit.Credit.CumulativeChainSha256, record.Completed.ValueChainSha256) {
		return 0, captureSnapshotConflict()
	}
	last, err := DecodeConfigRestoreTransferRecord(read.Values[2].Value)
	defer clear(last.Protected.Ciphertext)
	if err != nil || last.Owner != record.Owner || last.Sequence != record.Completed.CommittedRecordCount {
		return 0, captureSnapshotConflict()
	}
	if previous := read.Values[0]; previous != nil {
		stored, err := DecodeConfigRestoreGeneration(previous.Value)
		if err != nil || previous.Key != keys[0] || previous.Version != 1 || previous.ModRevision <= 0 ||
			stored.Owner != record.Owner || !proto.Equal(stored.Completed, record.Completed) {
			return 0, captureSnapshotConflict()
		}
		return previous.ModRevision, nil
	}
	conditions = append(conditions, etcdstore.Condition{Key: keys[0]},
		etcdstore.Condition{Key: ConfigTransferCursorKey(binding), ModRevision: cursor.Revision})
	result, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: keys[0], Value: encoded},
	})
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded || result.Revision <= 0 {
		return 0, captureSnapshotConflict()
	}
	return result.Revision, nil
}
