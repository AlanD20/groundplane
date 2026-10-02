package backupconfiguration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// PrepareConfigRestoreEntryPublication proves the exact metadata and original
// staged value selected by one ordinal. The caller must also fence the complete
// generation and native publication cursor in its live mutation transaction.
func PrepareConfigRestoreEntryPublication(ctx context.Context, store interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}, owner ConfigRestoreTransferOwner, ordinal uint32, record entries.Record, revision int64,
) ([]etcdstore.Condition, error) {
	if ctx == nil || store == nil || revision <= 0 || ValidateConfigRestoreTransferOwner(owner) != nil ||
		ordinal == 0 || record.EnvironmentID != owner.EnvironmentID || record.CurrentValueGenerationID != owner.GenerationID {
		return nil, captureSnapshotConflict()
	}
	metadata, err := entries.EncodeRecord(record)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(metadata)
	clear(metadata)
	receiptKey := ConfigRestoreEntryReceiptKey(owner, ordinal)
	valueKey := (ConfigRestoreEntryReceipt{Owner: owner, EntryID: record.Entry.ID, Secret: record.Entry.Secret}).ValueKey()
	keys := []string{receiptKey, valueKey}
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return nil, captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.Version != 1 || value.ModRevision <= 0 {
			return nil, captureSnapshotConflict()
		}
	}
	receipt, err := decodeConfigRestoreEntryReceipt(read.Values[0].Value)
	if err != nil || receipt.Owner != owner || receipt.Ordinal != ordinal || receipt.EntryID != record.Entry.ID ||
		receipt.Secret != record.Entry.Secret || receipt.MetadataSHA256 != hex.EncodeToString(digest[:]) ||
		read.Values[0].ModRevision != read.Values[1].ModRevision {
		return nil, captureSnapshotConflict()
	}
	stored, err := decodeConfigRestoreStoredValue(receipt, read.Values[1])
	if stored.Plain != nil {
		clear(stored.Plain.Content)
	}
	if stored.Secret != nil {
		clear(stored.Secret.Ciphertext)
	}
	if err != nil {
		return nil, err
	}
	return []etcdstore.Condition{{Key: receiptKey, ModRevision: read.Values[0].ModRevision},
		{Key: valueKey, ModRevision: read.Values[1].ModRevision}}, nil
}
