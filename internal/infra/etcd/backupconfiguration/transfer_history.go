package backupconfiguration

import (
	"context"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// VisitConfigTransferCredits reads every immutable receipt contiguously in
// bounded pages at the caller's fixed assignment-read revision. A truncated
// history, extra receipt, changed owner or compaction fails rather than falling
// back to the latest cursor.
func (repository *ConfigTransferRepository) VisitConfigTransferCredits(
	ctx context.Context,
	cursor etcdstore.Versioned[ConfigTransferCursor],
	visit func(*agentpb.BackupConfigCredit) error,
) error {
	if ctx == nil || cursor.ReadRevision <= 0 || cursor.Revision <= 0 || visit == nil ||
		ValidateConfigTransferOwner(cursor.Record.Owner) != nil {
		return captureSnapshotConflict()
	}
	owner := cursor.Record.Owner
	prefix := ConfigTransferCreditPrefix(owner.Binding)
	var previous *agentpb.BackupConfigCredit
	var sequence uint64
	var accepted uint64
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Revision: cursor.ReadRevision, Limit: 96})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != cursor.ReadRevision || len(page.Values) > 96 {
			return captureSnapshotConflict()
		}
		err = func() error {
			defer func() {
				for index := range page.Values {
					clear(page.Values[index].Value)
				}
			}()
			for _, value := range page.Values {
				if sequence == ^uint64(0) {
					return captureSnapshotConflict()
				}
				sequence++
				record, err := DecodeConfigTransferCredit(value.Value)
				if err != nil || value.Version != 1 || value.ModRevision <= 0 || value.ModRevision > cursor.Revision ||
					record.Owner != owner || record.Credit.CreditSequence != sequence || value.Key != ConfigTransferCreditKey(owner.Binding, sequence) {
					return captureSnapshotConflict()
				}
				credit := record.Credit
				if sequence == 1 {
					if credit.CommittedRecordSequence != 0 || credit.GetMetadataCredit() == nil {
						return captureSnapshotConflict()
					}
				} else if credit.CommittedRecordSequence <= previous.CommittedRecordSequence {
					return captureSnapshotConflict()
				}
				if credit.GetMetadataAccepted() != nil {
					if accepted != 0 || credit.NextOrdinal != 1 {
						return captureSnapshotConflict()
					}
					accepted = sequence
				} else if (credit.GetValueCredit() != nil) != (accepted != 0) {
					return captureSnapshotConflict()
				}
				if err := visit(credit); err != nil {
					return err
				}
				previous, start = credit, value.Key
			}
			return nil
		}()
		if err != nil {
			return err
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return captureSnapshotConflict()
		}
	}
	if sequence != cursor.Record.LastCreditSequence || accepted != cursor.Record.MetadataAcceptedCreditSequence {
		return captureSnapshotConflict()
	}
	return nil
}
