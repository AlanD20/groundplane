package backupconfiguration

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type retainedCaptureStore struct {
	values              map[string]*keyvalue.KeyValue
	descriptors, chunks []keyvalue.KeyValue
}

func (store *retainedCaptureStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if request.Revision != 0 && request.Revision <= 50 {
		return nil, errs.New(errs.KindCursorExpired, "old MVCC removed")
	}
	result := &keyvalue.GetManyResult{ReadRevision: 100, Values: make([]*keyvalue.KeyValue, len(request.Keys))}
	for index, key := range request.Keys {
		value := store.values[key]
		if value == nil {
			return nil, errs.New(errs.KindInternal, "unexpected source read")
		}
		copy := *value
		copy.Value = bytes.Clone(value.Value)
		result.Values[index] = &copy
	}
	return result, nil
}

func (store *retainedCaptureStore) Range(
	_ context.Context,
	request keyvalue.RangeRequest,
) (*keyvalue.RangeResult, error) {
	if request.Revision <= 50 {
		return nil, errs.New(errs.KindCursorExpired, "old MVCC removed")
	}
	var selected []keyvalue.KeyValue
	for _, value := range append(append([]keyvalue.KeyValue(nil), store.descriptors...), store.chunks...) {
		if len(value.Key) >= len(request.Prefix) && value.Key[:len(request.Prefix)] == request.Prefix {
			copy := value
			copy.Value = bytes.Clone(value.Value)
			selected = append(selected, copy)
		}
	}
	return &keyvalue.RangeResult{ReadRevision: 100, Values: selected}, nil
}

func (*retainedCaptureStore) MeasureTransaction(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionBudget, error) {
	return keyvalue.TransactionBudget{}, errs.New(errs.KindInternal, "reader requested mutation budget")
}

func (*retainedCaptureStore) Transact(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	return keyvalue.TransactionResult{}, errs.New(errs.KindInternal, "reader attempted mutation")
}

func TestRetainedCaptureReadsOriginalBytesAfterOldMVCCExpires(t *testing.T) {
	// HOST-17: compaction must not invalidate immutable captured config or
	// permit current desired values/late chunks to substitute for its old seal.
	now := testBackupConfigTime()
	snapshotID := ids.NewAt(ids.KindTask, now, 1)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2)
	sourceID := ids.NewAt(ids.KindBackupSource, now, 3)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 4)
	generationID := ids.NewAt(ids.KindConfig, now, 5)
	plain := []byte("captured-value")
	descriptor := []byte(`{"name":"CONFIG"}`)
	root := BackupConfigSnapshotRecord{SnapshotID: snapshotID, EnvironmentID: environmentID, SourceID: sourceID,
		State: BackupConfigSnapshotSealed, ReadRevision: 10, NextEntryOrdinal: 1, EntryCount: 1, DescriptorChunkCount: 1, ValueChunkCount: 1,
		PlainValueBytes: uint64(len(plain)), DescriptorChainSHA256: testBackupConfigSHA256([]byte("descriptor-chain")),
		StoredValueChainSHA256: testBackupConfigSHA256(
			[]byte("value-chain"),
		), StoredManifestSHA256: testBackupConfigSHA256([]byte("manifest")), CreatedAt: now, UpdatedAt: now}
	rootBytes, err := EncodeBackupConfigSnapshotRecord(root)
	if err != nil {
		t.Fatal(err)
	}
	entry := BackupConfigSnapshotEntryRecord{
		SnapshotID:              snapshotID,
		EntryID:                 entryID,
		EntryRevision:           9,
		ValueGenerationID:       generationID,
		ValueGenerationRevision: 9,
		DescriptorLength:        uint64(len(descriptor)),
		DescriptorSHA256:        testBackupConfigSHA256(descriptor),
		DescriptorChunks:        1,
		PlainValueLength:        uint64(len(plain)),
		PlainValueSHA256:        testBackupConfigSHA256(plain),
		ValueChunks:             1,
	}
	entryBytes, err := encodeBackupConfigSnapshotEntryRecord(entry)
	if err != nil {
		t.Fatal(err)
	}
	descriptorBytes, err := encodeBackupConfigSnapshotDescriptorChunkRecord(BackupConfigSnapshotDescriptorChunkRecord{
		SnapshotID: snapshotID, EntryID: entryID, ValueGenerationID: generationID, Length: uint32(len(descriptor)), SHA256: testBackupConfigSHA256(descriptor), Content: descriptor})
	if err != nil {
		t.Fatal(err)
	}
	valueBytes, err := encodeBackupConfigSnapshotValueChunkRecord(
		BackupConfigSnapshotValueChunkRecord{SnapshotID: snapshotID, EntryID: entryID,
			ValueGenerationID: generationID, Storage: BackupConfigChunkStoragePlain, Plain: &BackupConfigPlainChunkPayload{
				ContentLength: uint32(len(plain)), ContentSHA256: testBackupConfigSHA256(plain), Content: plain}},
	)
	if err != nil {
		t.Fatal(err)
	}
	rootKey := BackupConfigSnapshotKey(snapshotID)
	entryKey := backupConfigSnapshotEntryKey(snapshotID, 0)
	forward := BackupConfigSnapshotTaskReferenceKey(snapshotID, snapshotID)
	reverse := BackupConfigSnapshotReferenceTaskKey(snapshotID, snapshotID)
	store := &retainedCaptureStore{values: map[string]*keyvalue.KeyValue{
		rootKey: {
			Key:         rootKey,
			Value:       rootBytes,
			ModRevision: 40,
		}, entryKey: {Key: entryKey, Value: entryBytes, ModRevision: 30},
		forward: {
			Key:         forward,
			Value:       []byte(snapshotID),
			ModRevision: 20,
		}, reverse: {Key: reverse, Value: []byte(snapshotID), ModRevision: 20}},
		descriptors: []keyvalue.KeyValue{
			{Key: backupConfigSnapshotDescriptorChunkKey(snapshotID, 0, 0), Value: descriptorBytes, ModRevision: 30},
		},
		chunks: []keyvalue.KeyValue{
			{Key: backupConfigSnapshotValueChunkKey(snapshotID, 0, 0), Value: valueBytes, ModRevision: 30},
		}}
	repository, err := NewCaptureSnapshotRepository(
		store,
		func(_ context.Context, _ CaptureSnapshotWriteAuthority, revision int64) ([]keyvalue.Condition, error) {
			if revision != 100 {
				return nil, errs.New(errs.KindInternal, "ownership checked at an expired view")
			}
			return []keyvalue.Condition{
				{Key: "/task", ModRevision: 1},
				{Key: "/run", ModRevision: 1},
				{Key: "/source", ModRevision: 1},
				{Key: "/environment", ModRevision: 1},
			}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	reader := &CapturedSnapshotReader{
		repository: repository,
		authority: CaptureSnapshotWriteAuthority{TaskID: snapshotID, SnapshotID: snapshotID,
			EnvironmentID: environmentID, SourceID: sourceID, ReadRevision: 10},
		cursor: keyvalue.Versioned[BackupConfigSnapshotRecord]{Record: root, Revision: 40, ReadRevision: 100},
	}
	retained, err := reader.ReadEntry(t.Context(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer retained.Clear()
	if !bytes.Equal(retained.Descriptor, descriptor) || len(retained.Values) != 1 ||
		!bytes.Equal(retained.Values[0].Plain.Content, plain) {
		t.Fatal("captured bytes were replaced")
	}
	store.chunks[0].ModRevision = 41
	if entry, err := reader.ReadEntry(t.Context(), 0); err == nil {
		entry.Clear()
		t.Fatal("late value chunk substituted behind old seal")
	}
}
