package backupconfiguration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"google.golang.org/protobuf/proto"
)

// StageConfigRestoreEntry writes an immutable value and its cursor together.
// It does not publish Entry metadata or change the Environment's desired head.
// Existing receipts return the original ciphertext for independent verification.
func (repository *ConfigTransferRepository) StageConfigRestoreEntry(ctx context.Context,
	seal etcdstore.Versioned[ConfigRestoreGenerationRecord], ordinal uint32,
	record entries.Record, selectedSize uint64, selectedSHA256 string,
	generation entries.EntryValueGeneration,
) (entries.EntryValueGeneration, error) {
	var zero entries.EntryValueGeneration
	owner := seal.Record.Owner
	if ctx == nil || repository == nil || repository.guard == nil || repository.restoreGuard == nil ||
		seal.Revision <= 0 || seal.Record.Completed == nil || seal.Record.Completed.Content == nil ||
		ordinal == 0 || ordinal > seal.Record.Completed.Content.EntryCount ||
		record.EnvironmentID != owner.EnvironmentID || record.CurrentValueGenerationID != owner.GenerationID {
		return zero, captureSnapshotConflict()
	}
	sealValue, err := EncodeConfigRestoreGeneration(seal.Record)
	clear(sealValue)
	if err != nil {
		return zero, err
	}
	metadata, err := entries.EncodeRecord(record)
	if err != nil {
		return zero, err
	}
	defer clear(metadata)
	metadataDigest := sha256.Sum256(metadata)
	key, encoded, err := entries.PrepareEntryGeneration(record, generation)
	if err != nil {
		return zero, err
	}
	defer clear(encoded)
	storedDigest := sha256.Sum256(encoded)
	receipt := ConfigRestoreEntryReceipt{Owner: owner, Ordinal: ordinal, EntryID: record.Entry.ID,
		Secret: record.Entry.Secret, MetadataSHA256: hex.EncodeToString(metadataDigest[:]),
		ValueSizeBytes: selectedSize, ValueSHA256: selectedSHA256, StoredSHA256: hex.EncodeToString(storedDigest[:])}
	if receipt.Secret {
		// Secret plaintext evidence stays in the protected transfer, not this index.
		receipt.ValueSizeBytes, receipt.ValueSHA256 = 0, ""
	}
	receiptValue, err := encodeConfigRestoreEntryReceipt(receipt)
	if err != nil || receipt.ValueKey() != key {
		return zero, captureSnapshotConflict()
	}
	defer clear(receiptValue)
	receiptKey, cursorKey := ConfigRestoreEntryReceiptKey(owner, ordinal), ConfigRestoreEntryCursorKey(owner)
	headKey := blueprints.EnvironmentBlueprintHeadKey(record.EnvironmentID)
	keys := []string{ConfigRestoreGenerationKey(owner), receiptKey, cursorKey, key, headKey}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return zero, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) || read.Values[0] == nil {
		return zero, captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	conditions, err := repository.guard(ctx, owner.Transfer, read.ReadRevision)
	if err != nil {
		return zero, err
	}
	more, err := repository.restoreGuard(ctx, owner, read.ReadRevision)
	if err != nil {
		return zero, err
	}
	conditions = append(conditions, more...)
	storedSeal, err := DecodeConfigRestoreGeneration(read.Values[0].Value)
	if err != nil || read.Values[0].Key != keys[0] || read.Values[0].Version != 1 ||
		read.Values[0].ModRevision != seal.Revision ||
		storedSeal.Owner != owner ||
		!proto.Equal(storedSeal.Completed, seal.Record.Completed) {
		return zero, captureSnapshotConflict()
	}
	if existing := read.Values[1]; existing != nil {
		stored, err := decodeConfigRestoreEntryReceipt(existing.Value)
		if err != nil || existing.Key != receiptKey || existing.Version != 1 || stored.Owner != owner ||
			stored.Ordinal != ordinal || stored.EntryID != receipt.EntryID || stored.Secret != receipt.Secret ||
			stored.MetadataSHA256 != receipt.MetadataSHA256 || stored.ValueSizeBytes != receipt.ValueSizeBytes || stored.ValueSHA256 != receipt.ValueSHA256 ||
			read.Values[2] == nil || read.Values[3] == nil {
			return zero, captureSnapshotConflict()
		}
		cursor, err := decodeConfigRestoreEntryCursor(read.Values[2].Value)
		if err != nil || read.Values[2].Key != cursorKey || read.Values[2].ModRevision <= 0 ||
			cursor.Owner != owner || cursor.LastOrdinal < ordinal ||
			existing.ModRevision != read.Values[3].ModRevision || existing.ModRevision > read.Values[2].ModRevision {
			return zero, captureSnapshotConflict()
		}
		return decodeConfigRestoreStoredValue(stored, read.Values[3])
	}
	if read.Values[3] != nil || read.Values[4] == nil || read.Values[4].Key != headKey ||
		read.Values[4].ModRevision != owner.BaselineHeadRevision {
		return zero, captureSnapshotConflict()
	}
	head, err := idempotency.DecodeTaskReference(read.Values[4].Value)
	if err != nil || head != owner.BaselineRevisionID {
		return zero, captureSnapshotConflict()
	}
	previousRevision := int64(0)
	if previous := read.Values[2]; previous != nil {
		cursor, err := decodeConfigRestoreEntryCursor(previous.Value)
		if err != nil || previous.Key != cursorKey || cursor.Owner != owner ||
			cursor.LastOrdinal+1 != ordinal || cursor.LastEntryID >= record.Entry.ID {
			return zero, captureSnapshotConflict()
		}
		previousRevision = previous.ModRevision
	} else if ordinal != 1 {
		return zero, captureSnapshotConflict()
	}
	cursorValue, err := encodeConfigRestoreEntryCursor(
		ConfigRestoreEntryCursor{Owner: owner, LastOrdinal: ordinal, LastEntryID: record.Entry.ID},
	)
	if err != nil {
		return zero, err
	}
	defer clear(cursorValue)
	conditions = append(conditions, etcdstore.Condition{Key: keys[0], ModRevision: seal.Revision},
		etcdstore.Condition{Key: receiptKey}, etcdstore.Condition{Key: cursorKey, ModRevision: previousRevision},
		etcdstore.Condition{Key: key}, etcdstore.Condition{Key: headKey, ModRevision: owner.BaselineHeadRevision})
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: encoded},
		{Type: etcdstore.MutationPut, Key: receiptKey, Value: receiptValue},
		{Type: etcdstore.MutationPut, Key: cursorKey, Value: cursorValue}}
	budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
	if err != nil {
		return zero, err
	}
	if budget.Operations > 32 || budget.Bytes > 768<<10 {
		return zero, captureSnapshotConflict()
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return zero, err
	}
	if !result.Succeeded || result.Revision <= 0 {
		return zero, captureSnapshotConflict()
	}
	return decodeConfigRestoreStoredValue(receipt, &etcdstore.KeyValue{Key: key, Value: encoded, Version: 1})
}

func decodeConfigRestoreStoredValue(
	receipt ConfigRestoreEntryReceipt,
	value *etcdstore.KeyValue,
) (entries.EntryValueGeneration, error) {
	var result entries.EntryValueGeneration
	if value == nil || value.Key != receipt.ValueKey() || value.Version != 1 {
		return result, captureSnapshotConflict()
	}
	digest := sha256.Sum256(value.Value)
	if hex.EncodeToString(digest[:]) != receipt.StoredSHA256 {
		return result, captureSnapshotConflict()
	}
	if receipt.Secret {
		stored, err := entryvalues.DecodeSecret(value.Value)
		if err != nil || stored.EnvironmentID != receipt.Owner.EnvironmentID || stored.EntryID != receipt.EntryID ||
			stored.GenerationID != receipt.Owner.GenerationID {
			clear(stored.Ciphertext)
			return result, captureSnapshotConflict()
		}
		result.Secret = &stored
	} else {
		stored, err := entryvalues.DecodePlain(value.Value)
		if err != nil || stored.EnvironmentID != receipt.Owner.EnvironmentID || stored.EntryID != receipt.EntryID || stored.GenerationID != receipt.Owner.GenerationID ||
			uint64(len(stored.Content)) != receipt.ValueSizeBytes || stored.PlaintextSHA256 != receipt.ValueSHA256 {
			clear(stored.Content)
			return result, captureSnapshotConflict()
		}
		result.Plain = &stored
	}
	return result, nil
}
