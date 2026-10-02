package backupconfiguration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

// CapturedSnapshotReader reads immutable children at the original seal
// revision. Current Task admission is distinct from this byte authority, so a
// retry can read retained bytes without rebuilding from current Entries.
type CapturedSnapshotReader struct {
	repository *CaptureSnapshotRepository
	authority  CaptureSnapshotWriteAuthority
	cursor     etcdstore.Versioned[BackupConfigSnapshotRecord]
	evidence   CaptureSnapshotEvidence
}

// CapturedSnapshotEntry owns its descriptor and plain/ciphertext chunk buffers.
// Protected values are never decrypted by this infrastructure owner.
type CapturedSnapshotEntry struct {
	Record     BackupConfigSnapshotEntryRecord
	Descriptor []byte
	Values     []BackupConfigSnapshotValueChunkRecord
}

func (entry *CapturedSnapshotEntry) Clear() {
	if entry == nil {
		return
	}
	clear(entry.Descriptor)
	entry.Descriptor = nil
	for _, chunk := range entry.Values {
		if chunk.Plain != nil {
			clear(chunk.Plain.Content)
		}
		clearBackupConfigProtectedChunk(chunk.Protected)
	}
	entry.Values = nil
}

// OpenCapturedSnapshot proves the complete stored count/chain inventory before
// returning a reader. Reads are bounded per Entry; no aggregate values are
// retained. Compaction is an error, never permission to read latest children.
func (repository *CaptureSnapshotRepository) OpenCapturedSnapshot(
	ctx context.Context,
	authority CaptureSnapshotWriteAuthority,
) (*CapturedSnapshotReader, error) {
	authority.Expected = proto.CloneOf(authority.Expected)
	cursor, _, err := repository.readClaim(ctx, authority)
	if err != nil {
		return nil, err
	}
	if cursor.Record.State != BackupConfigSnapshotSealed || cursor.Record.EntryCount > backupconfig.MaxEntries ||
		authority.Expected == nil || authority.Expected.Content == nil {
		return nil, capturedSnapshotCorrupt()
	}
	keys := []string{BackupConfigSnapshotKey(authority.SnapshotID), captureSnapshotEvidenceKey(authority.SnapshotID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: cursor.Revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != cursor.Revision || len(read.Values) != 2 {
		return nil, capturedSnapshotCorrupt()
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision != cursor.Revision {
			return nil, capturedSnapshotCorrupt()
		}
	}
	sealed, err := DecodeBackupConfigSnapshotRecord(read.Values[0].Value)
	if err != nil || sealed != cursor.Record {
		return nil, capturedSnapshotCorrupt()
	}
	evidence, err := recordcodec.Decode[CaptureSnapshotEvidence](read.Values[1].Value, "backup-config-capture-evidence")
	if err != nil {
		return nil, capturedSnapshotCorrupt()
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(authority.Expected)
	if err != nil {
		return nil, capturedSnapshotCorrupt()
	}
	defer clear(canonical)
	if evidence.SnapshotID != sealed.SnapshotID || evidence.EnvironmentID != sealed.EnvironmentID ||
		evidence.SourceID != sealed.SourceID ||
		evidence.ReadRevision != sealed.ReadRevision ||
		uint64(evidence.EntryCount) != sealed.EntryCount ||
		evidence.EntryCount != authority.Expected.Content.EntryCount ||
		evidence.MetadataProtoBytes != authority.Expected.MetadataProtoBytes ||
		evidence.ManifestSHA256 != sealed.StoredManifestSHA256 ||
		evidence.ManifestSHA256 != hex.EncodeToString(authority.Expected.Content.ManifestSha256) ||
		evidence.SourceSizeBytes != authority.Expected.Content.SourceSizeBytes ||
		!validBackupConfigSHA256(evidence.SourceSHA256) ||
		!bytes.Equal(evidence.Authority, canonical) {
		return nil, capturedSnapshotCorrupt()
	}
	reader := &CapturedSnapshotReader{repository: repository, authority: authority, cursor: cursor, evidence: evidence}
	if err := reader.verifyInventory(ctx); err != nil {
		return nil, err
	}
	return reader, nil
}

func (reader *CapturedSnapshotReader) EntryCount() uint64 { return reader.cursor.Record.EntryCount }

func (reader *CapturedSnapshotReader) Evidence() CaptureSnapshotEvidence {
	evidence := reader.evidence
	evidence.Authority = bytes.Clone(evidence.Authority)
	return evidence
}

// ReadEntry rechecks current ownership, then returns one complete Entry from
// the pinned seal view. Persistent ordinals are zero-based; protocol ordinals
// are one-based and are verified by the Controller metadata owner.
func (reader *CapturedSnapshotReader) ReadEntry(ctx context.Context, ordinal uint64) (*CapturedSnapshotEntry, error) {
	if reader == nil {
		return nil, capturedSnapshotCorrupt()
	}
	current, _, err := reader.repository.readClaim(ctx, reader.authority)
	if err != nil {
		return nil, err
	}
	if current.Revision != reader.cursor.Revision || current.Record != reader.cursor.Record {
		return nil, capturedSnapshotCorrupt()
	}
	return reader.readEntry(ctx, ordinal, nil)
}

type capturedSnapshotInventory struct {
	entries         uint64
	descriptors     uint64
	values          uint64
	plainBytes      uint64
	metadataBytes   uint64
	descriptorChain string
	valueChain      string
	lastEntryID     string
}

func (reader *CapturedSnapshotReader) verifyInventory(ctx context.Context) error {
	var inventory capturedSnapshotInventory
	for ordinal := uint64(0); ordinal < reader.EntryCount(); ordinal++ {
		entry, err := reader.readEntry(ctx, ordinal, &inventory)
		if err != nil {
			return err
		}
		entry.Clear()
	}
	start := ""
	if reader.EntryCount() != 0 {
		start = backupConfigSnapshotEntryKey(reader.authority.SnapshotID, reader.EntryCount()-1)
	}
	remaining, err := reader.fixedRange(ctx, backupConfigSnapshotEntryPrefix(reader.authority.SnapshotID), start, 1)
	if err != nil {
		return err
	}
	defer clearCapturedRange(remaining)
	if len(remaining.Values) != 0 || remaining.More || inventory.entries != reader.EntryCount() ||
		inventory.descriptors != reader.cursor.Record.DescriptorChunkCount || inventory.values != reader.cursor.Record.ValueChunkCount ||
		inventory.plainBytes != reader.cursor.Record.PlainValueBytes || inventory.metadataBytes != reader.evidence.MetadataProtoBytes ||
		inventory.descriptorChain != reader.cursor.Record.DescriptorChainSHA256 || inventory.valueChain != reader.cursor.Record.StoredValueChainSHA256 {
		return capturedSnapshotCorrupt()
	}
	return nil
}

func (reader *CapturedSnapshotReader) readEntry(
	ctx context.Context,
	ordinal uint64,
	inventory *capturedSnapshotInventory,
) (_ *CapturedSnapshotEntry, resultErr error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	if ordinal >= reader.EntryCount() {
		return nil, capturedSnapshotCorrupt()
	}
	key := backupConfigSnapshotEntryKey(reader.authority.SnapshotID, ordinal)
	read, err := reader.repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{key}, Revision: reader.cursor.Revision},
	)
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != reader.cursor.Revision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > reader.cursor.Revision {
		return nil, capturedSnapshotCorrupt()
	}
	defer etcdstore.ClearValues(read.Values)
	record, err := decodeBackupConfigSnapshotEntryRecord(read.Values[0].Value)
	if err != nil || record.SnapshotID != reader.authority.SnapshotID || record.EntryOrdinal != ordinal ||
		record.DescriptorLength > backupconfig.MaxCanonicalEntryBytes || record.DescriptorChunks != 1 ||
		record.PlainValueLength > backupconfig.MaxSelectedValueBytes || record.ValueChunks > uint32((backupconfig.MaxSelectedValueBytes+MaximumBackupConfigChunkPayloadBytes-1)/MaximumBackupConfigChunkPayloadBytes) {
		return nil, capturedSnapshotCorrupt()
	}
	entry := &CapturedSnapshotEntry{Record: record}
	defer func() {
		if resultErr != nil {
			entry.Clear()
		}
	}()
	descriptors, err := reader.fixedRange(
		ctx,
		backupConfigSnapshotDescriptorChunkPrefix(record.SnapshotID, ordinal),
		"",
		int64(record.DescriptorChunks)+1,
	)
	if err != nil {
		return nil, err
	}
	defer clearCapturedRange(descriptors)
	if descriptors.More || len(descriptors.Values) != int(record.DescriptorChunks) {
		return nil, capturedSnapshotCorrupt()
	}
	for index, raw := range descriptors.Values {
		chunk, err := decodeBackupConfigSnapshotDescriptorChunkRecord(raw.Value)
		if err != nil || raw.ModRevision != read.Values[0].ModRevision ||
			raw.Key != backupConfigSnapshotDescriptorChunkKey(record.SnapshotID, ordinal, uint32(index)) ||
			chunk.SnapshotID != record.SnapshotID ||
			chunk.EntryOrdinal != ordinal ||
			chunk.EntryID != record.EntryID ||
			chunk.ValueGenerationID != record.ValueGenerationID ||
			chunk.Secret != record.Secret ||
			chunk.ChunkOrdinal != uint32(index) {
			clear(chunk.Content)
			return nil, capturedSnapshotCorrupt()
		}
		entry.Descriptor = append(entry.Descriptor, chunk.Content...)
		clear(chunk.Content)
		if inventory != nil {
			inventory.descriptorChain = captureStoredChunkChain(
				"descriptor",
				inventory.descriptorChain,
				raw.Key,
				raw.Value,
			)
			inventory.descriptors++
		}
	}
	descriptorSHA := sha256.Sum256(entry.Descriptor)
	if uint64(len(entry.Descriptor)) != record.DescriptorLength ||
		hex.EncodeToString(descriptorSHA[:]) != record.DescriptorSHA256 {
		return nil, capturedSnapshotCorrupt()
	}
	values, err := reader.fixedRange(
		ctx,
		backupConfigSnapshotValueChunkPrefix(record.SnapshotID, ordinal),
		"",
		int64(record.ValueChunks)+1,
	)
	if err != nil {
		return nil, err
	}
	defer clearCapturedRange(values)
	if values.More || len(values.Values) != int(record.ValueChunks) {
		return nil, capturedSnapshotCorrupt()
	}
	plainHash := sha256.New()
	var plainBytes uint64
	for index, raw := range values.Values {
		chunk, err := decodeBackupConfigSnapshotValueChunkRecord(raw.Value)
		if err != nil || raw.ModRevision != read.Values[0].ModRevision ||
			raw.Key != backupConfigSnapshotValueChunkKey(record.SnapshotID, ordinal, uint32(index)) ||
			chunk.SnapshotID != record.SnapshotID ||
			chunk.EntryOrdinal != ordinal ||
			chunk.EntryID != record.EntryID ||
			chunk.ValueGenerationID != record.ValueGenerationID ||
			chunk.Secret != record.Secret ||
			chunk.ChunkOrdinal != uint32(index) {
			if chunk.Plain != nil {
				clear(chunk.Plain.Content)
			}
			clearBackupConfigProtectedChunk(chunk.Protected)
			return nil, capturedSnapshotCorrupt()
		}
		entry.Values = append(entry.Values, chunk)
		if chunk.Plain != nil {
			_, _ = plainHash.Write(chunk.Plain.Content)
			plainBytes += uint64(len(chunk.Plain.Content))
		}
		if inventory != nil {
			inventory.valueChain = captureStoredChunkChain("value", inventory.valueChain, raw.Key, raw.Value)
			inventory.values++
		}
	}
	if !record.Secret &&
		(plainBytes != record.PlainValueLength || hex.EncodeToString(plainHash.Sum(nil)) != record.PlainValueSHA256) {
		return nil, capturedSnapshotCorrupt()
	}
	if inventory != nil {
		if record.EntryID <= inventory.lastEntryID {
			return nil, capturedSnapshotCorrupt()
		}
		inventory.lastEntryID = record.EntryID
		inventory.entries++
		inventory.plainBytes += plainBytes
		inventory.metadataBytes += uint64(len(entry.Descriptor))
	}
	return entry, nil
}

func (reader *CapturedSnapshotReader) fixedRange(
	ctx context.Context,
	prefix, start string,
	limit int64,
) (*etcdstore.RangeResult, error) {
	read, err := reader.repository.store.Range(
		ctx,
		etcdstore.RangeRequest{Prefix: prefix, StartExclusive: start, Limit: limit, Revision: reader.cursor.Revision},
	)
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != reader.cursor.Revision {
		return nil, capturedSnapshotCorrupt()
	}
	return read, nil
}

func clearCapturedRange(read *etcdstore.RangeResult) {
	if read != nil {
		for _, value := range read.Values {
			clear(value.Value)
		}
	}
}

func capturedSnapshotCorrupt() error {
	return errs.New(errs.KindInternal, "sealed Config snapshot inventory or immutable bytes are inconsistent")
}
