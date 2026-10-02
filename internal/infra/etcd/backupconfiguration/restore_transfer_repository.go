package backupconfiguration

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CommitConfigRestoreTransfer acknowledges only records supplied by the
// restore receiver after validation. Both their protected bytes and the native
// credit are committed in one bounded transaction under the sealed generation.
// The caller retains the prepared batch across an ambiguous transaction result;
// it must not create different ciphertext for the same immutable record.
func (repository *ConfigTransferRepository) CommitConfigRestoreTransfer(
	ctx context.Context,
	batch ConfigRestoreTransferBatch,
	credit *agentpb.BackupConfigCredit,
) (int64, error) {
	if ValidateConfigRestoreTransferOwner(batch.Owner) != nil ||
		len(batch.Records) > int(backupconfigtransfer.MetadataCreditRecords) {
		return 0, captureSnapshotConflict()
	}
	return repository.commitConfigTransferCredit(ctx, batch.Owner.Transfer, credit, &batch)
}

func prepareConfigRestoreRecords(
	batch ConfigRestoreTransferBatch,
	previousSequence uint64,
	credit *agentpb.BackupConfigCredit,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if credit.CommittedRecordSequence < previousSequence ||
		credit.CommittedRecordSequence-previousSequence != uint64(len(batch.Records)) ||
		len(batch.Records) > int(backupconfigtransfer.MetadataCreditRecords) ||
		(credit.GetValueCredit() != nil && len(batch.Records) > int(backupconfigtransfer.ValueCreditRecords)) {
		return nil, nil, captureSnapshotConflict()
	}
	conditions := make([]etcdstore.Condition, 0, len(batch.Records))
	mutations := make([]etcdstore.Mutation, 0, len(batch.Records))
	for index, record := range batch.Records {
		if record.Owner != batch.Owner || record.Sequence != previousSequence+uint64(index)+1 {
			etcdstore.ClearMutationValues(mutations)
			return nil, nil, captureSnapshotConflict()
		}
		encoded, err := EncodeConfigRestoreTransferRecord(record)
		if err != nil {
			etcdstore.ClearMutationValues(mutations)
			return nil, nil, err
		}
		key := ConfigRestoreTransferRecordKey(batch.Owner.Transfer.Binding, record.Sequence)
		conditions = append(conditions, etcdstore.Condition{Key: key})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded})
	}
	return conditions, mutations, nil
}

func (repository *ConfigTransferRepository) verifyConfigRestoreBatch(
	ctx context.Context,
	batch ConfigRestoreTransferBatch,
	credit *agentpb.BackupConfigCredit,
	revision int64,
) error {
	first := uint64(0)
	if credit.CreditSequence > 1 {
		key := ConfigTransferCreditKey(batch.Owner.Transfer.Binding, credit.CreditSequence-1)
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
		if err != nil {
			return err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
			return captureSnapshotConflict()
		}
		defer etcdstore.ClearValues(read.Values)
		previous, err := DecodeConfigTransferCredit(read.Values[0].Value)
		if err != nil || read.Values[0].Key != key || read.Values[0].Version != 1 || read.Values[0].ModRevision <= 0 ||
			previous.Owner != batch.Owner.Transfer || previous.Credit.CreditSequence != credit.CreditSequence-1 {
			return captureSnapshotConflict()
		}
		first = previous.Credit.CommittedRecordSequence
	}
	_, mutations, err := prepareConfigRestoreRecords(batch, first, credit)
	if err != nil {
		return err
	}
	defer etcdstore.ClearMutationValues(mutations)
	// Only initial credit may be replayed without its complete retained batch.
	if len(mutations) == 0 {
		if credit.CreditSequence != 1 {
			return captureSnapshotConflict()
		}
		return nil
	}
	keys := make([]string, len(mutations))
	for index := range mutations {
		keys[index] = mutations[index].Key
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.Version != 1 || value.ModRevision <= 0 ||
			!bytes.Equal(value.Value, mutations[index].Value) {
			return captureSnapshotConflict()
		}
	}
	return nil
}

// VisitConfigRestoreTransferRecords reads one committed interval in bounded
// pages at the cursor's fixed MVCC view. Missing, changed or extra records fail
// closed; replay cannot silently reread latest state after compaction.
func (repository *ConfigTransferRepository) VisitConfigRestoreTransferRecords(
	ctx context.Context,
	owner ConfigRestoreTransferOwner,
	cursor etcdstore.Versioned[ConfigTransferCursor],
	first, last uint64,
	visit func(ConfigRestoreTransferRecord) error,
) error {
	if etcdstore.ValidateContext(ctx) != nil || ValidateConfigRestoreTransferOwner(owner) != nil ||
		cursor.Record.Owner != owner.Transfer || cursor.ReadRevision <= 0 || cursor.Revision <= 0 ||
		first == 0 || first > last || visit == nil {
		return captureSnapshotConflict()
	}
	if _, err := repository.guard(ctx, owner.Transfer, cursor.ReadRevision); err != nil {
		return err
	}
	if _, err := repository.restoreGuard(ctx, owner, cursor.ReadRevision); err != nil {
		return err
	}
	creditRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{ConfigTransferCreditKey(owner.Transfer.Binding, cursor.Record.LastCreditSequence)},
		Revision: cursor.ReadRevision,
	})
	if err != nil {
		return err
	}
	if creditRead == nil || creditRead.ReadRevision != cursor.ReadRevision || len(creditRead.Values) != 1 ||
		creditRead.Values[0] == nil {
		return captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(creditRead.Values)
	value := creditRead.Values[0]
	credit, err := DecodeConfigTransferCredit(value.Value)
	if err != nil || credit.Owner != owner.Transfer || value.Version != 1 || value.ModRevision <= 0 ||
		value.ModRevision > cursor.Revision || credit.Credit.CreditSequence != cursor.Record.LastCreditSequence ||
		last > credit.Credit.CommittedRecordSequence {
		return captureSnapshotConflict()
	}
	prefix := ConfigRestoreTransferRecordPrefix(owner.Transfer.Binding)
	sequence := first
	start := ConfigRestoreTransferRecordKey(owner.Transfer.Binding, first-1)
	for sequence <= last {
		limit := min(uint64(8), last-sequence+1)
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Revision: cursor.ReadRevision, Limit: int64(limit),
		})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != cursor.ReadRevision || len(page.Values) != int(limit) {
			return captureSnapshotConflict()
		}
		err = func() error {
			defer func() {
				for index := range page.Values {
					clear(page.Values[index].Value)
				}
			}()
			for _, value := range page.Values {
				record, err := DecodeConfigRestoreTransferRecord(value.Value)
				if err != nil || record.Owner != owner || record.Sequence != sequence || value.Version != 1 ||
					value.ModRevision <= 0 || value.ModRevision > cursor.Revision ||
					value.Key != ConfigRestoreTransferRecordKey(owner.Transfer.Binding, sequence) {
					clear(record.Protected.Ciphertext)
					return captureSnapshotConflict()
				}
				err = visit(record)
				clear(record.Protected.Ciphertext)
				if err != nil {
					return err
				}
				sequence++
				start = value.Key
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
