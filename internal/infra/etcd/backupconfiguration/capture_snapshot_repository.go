package backupconfiguration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type captureSnapshotStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	MeasureTransaction(
		context.Context,
		[]etcdstore.Condition,
		[]etcdstore.Mutation,
	) (etcdstore.TransactionBudget, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// CaptureSnapshotRepository appends finalized Entries atomically with their
// cursor. It cannot create ownership: Task publication must establish both
// snapshot references and the Building cursor first.
type CaptureSnapshotWriteAuthority struct {
	TaskID        string
	SnapshotID    string
	EnvironmentID string
	SourceID      string
	ReadRevision  int64
	Expected      *agentpb.BackupConfigCaptureAuthority
}

// CaptureSnapshotAuthorityGuard validates the current Task/run, immutable
// procedure and Environment lock at the supplied snapshot-read revision. It
// returns their positive-revision CAS fences; the Task codec remains owned by
// the root repository, rather than being copied into this child package.
type CaptureSnapshotAuthorityGuard func(context.Context, CaptureSnapshotWriteAuthority, int64) ([]etcdstore.Condition, error)

type CaptureSnapshotRepository struct {
	store captureSnapshotStore
	guard CaptureSnapshotAuthorityGuard
}

func NewCaptureSnapshotRepository(
	store captureSnapshotStore,
	guard CaptureSnapshotAuthorityGuard,
) (*CaptureSnapshotRepository, error) {
	if store == nil || guard == nil {
		return nil, errs.New(errs.KindInternal, "Config snapshot store is required")
	}
	return &CaptureSnapshotRepository{store: store, guard: guard}, nil
}

type CaptureSnapshotEvidence struct {
	SnapshotID         string `json:"snapshot_id"`
	EnvironmentID      string `json:"environment_id"`
	SourceID           string `json:"source_id"`
	ReadRevision       int64  `json:"read_revision"`
	EntryCount         uint32 `json:"entry_count"`
	MetadataProtoBytes uint64 `json:"metadata_proto_bytes"`
	ManifestSHA256     string `json:"manifest_sha256"`
	SourceSizeBytes    uint64 `json:"source_size_bytes"`
	SourceSHA256       string `json:"source_sha256"`
	Authority          []byte `json:"authority"`
}

func captureSnapshotEvidenceKey(snapshotID string) string {
	return backupConfigSnapshotPrefix + snapshotID + "/sealed-evidence"
}

func (repository *CaptureSnapshotRepository) readClaim(
	ctx context.Context,
	authority CaptureSnapshotWriteAuthority,
) (etcdstore.Versioned[BackupConfigSnapshotRecord], []etcdstore.Condition, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, nil, err
	}
	snapshotID := authority.SnapshotID
	keys := []string{
		BackupConfigSnapshotKey(snapshotID),
		BackupConfigSnapshotTaskReferenceKey(authority.TaskID, snapshotID),
		BackupConfigSnapshotReferenceTaskKey(snapshotID, authority.TaskID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, nil, err
	}
	if read == nil || len(read.Values) != len(keys) {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, nil, captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, nil, captureSnapshotConflict()
		}
	}
	if !bytes.Equal(read.Values[1].Value, []byte(snapshotID)) ||
		!bytes.Equal(read.Values[2].Value, []byte(authority.TaskID)) {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, nil, captureSnapshotConflict()
	}
	cursor, err := DecodeBackupConfigSnapshotRecord(read.Values[0].Value)
	if err != nil || cursor.SnapshotID != snapshotID || cursor.State == BackupConfigSnapshotUninitialized ||
		cursor.EnvironmentID != authority.EnvironmentID || cursor.SourceID != authority.SourceID || cursor.ReadRevision != authority.ReadRevision {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, nil, captureSnapshotConflict()
	}
	conditions := make([]etcdstore.Condition, len(keys))
	for index, value := range read.Values {
		conditions[index] = etcdstore.Condition{Key: keys[index], ModRevision: value.ModRevision}
	}
	liveConditions, err := repository.guard(ctx, authority, read.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, nil, err
	}
	if len(liveConditions) != 4 {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, nil, captureSnapshotConflict()
	}
	for _, condition := range liveConditions {
		if condition.ModRevision <= 0 || condition.Prefix {
			return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, nil, captureSnapshotConflict()
		}
	}
	conditions = append(conditions, liveConditions...)
	return etcdstore.Versioned[BackupConfigSnapshotRecord]{
		Record:       cursor,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, conditions, nil
}

func (repository *CaptureSnapshotRepository) ReadCursor(
	ctx context.Context,
	authority CaptureSnapshotWriteAuthority,
) (etcdstore.Versioned[BackupConfigSnapshotRecord], error) {
	cursor, _, err := repository.readClaim(ctx, authority)
	return cursor, err
}

// AppendEntry writes one complete, bounded Entry, all of its chunks and the
// resulting cursor in one compare-and-swap. A crash cannot expose half an Entry.
func (repository *CaptureSnapshotRepository) AppendEntry(
	ctx context.Context,
	authority CaptureSnapshotWriteAuthority,
	expected etcdstore.Versioned[BackupConfigSnapshotRecord],
	entry BackupConfigSnapshotEntryRecord,
	descriptors []BackupConfigSnapshotDescriptorChunkRecord,
	values []BackupConfigSnapshotValueChunkRecord,
) (etcdstore.Versioned[BackupConfigSnapshotRecord], error) {
	cursor, conditions, err := repository.readClaim(ctx, authority)
	if err != nil {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, err
	}
	if cursor.Revision != expected.Revision || cursor.Record.State != BackupConfigSnapshotBuilding ||
		entry.SnapshotID != cursor.Record.SnapshotID || entry.EntryOrdinal != cursor.Record.EntryCount ||
		cursor.Record.EntryCount >= backupconfig.MaxEntries || len(descriptors) != int(entry.DescriptorChunks) || len(values) != int(entry.ValueChunks) ||
		len(descriptors)+len(values) > MaximumBackupConfigChunksPerTransaction {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, captureSnapshotConflict()
	}
	entryBytes, err := encodeBackupConfigSnapshotEntryRecord(entry)
	if err != nil {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, err
	}
	mutations := []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   backupConfigSnapshotEntryKey(entry.SnapshotID, entry.EntryOrdinal),
			Value: entryBytes,
		},
	}
	defer func() { etcdstore.ClearMutationValues(mutations) }()
	conditions = append(conditions, etcdstore.Condition{Key: mutations[0].Key, ModRevision: 0})
	descriptorHash := sha256.New()
	var descriptorLength uint64
	for ordinal, chunk := range descriptors {
		if chunk.SnapshotID != entry.SnapshotID || chunk.EntryOrdinal != entry.EntryOrdinal ||
			chunk.EntryID != entry.EntryID ||
			chunk.ValueGenerationID != entry.ValueGenerationID ||
			chunk.Secret != entry.Secret ||
			chunk.ChunkOrdinal != uint32(ordinal) {
			return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, captureSnapshotConflict()
		}
		encoded, err := encodeBackupConfigSnapshotDescriptorChunkRecord(chunk)
		if err != nil {
			return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, err
		}
		key := backupConfigSnapshotDescriptorChunkKey(entry.SnapshotID, entry.EntryOrdinal, chunk.ChunkOrdinal)
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded})
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: 0})
		_, _ = descriptorHash.Write(chunk.Content)
		descriptorLength += uint64(len(chunk.Content))
		cursor.Record.DescriptorChainSHA256 = captureStoredChunkChain(
			"descriptor",
			cursor.Record.DescriptorChainSHA256,
			key,
			encoded,
		)
	}
	if descriptorLength != entry.DescriptorLength ||
		hex.EncodeToString(descriptorHash.Sum(nil)) != entry.DescriptorSHA256 {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, captureSnapshotConflict()
	}
	plainHash := sha256.New()
	var plainLength uint64
	for ordinal, chunk := range values {
		if chunk.SnapshotID != entry.SnapshotID || chunk.EntryOrdinal != entry.EntryOrdinal ||
			chunk.EntryID != entry.EntryID ||
			chunk.ValueGenerationID != entry.ValueGenerationID ||
			chunk.Secret != entry.Secret ||
			chunk.ChunkOrdinal != uint32(ordinal) {
			return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, captureSnapshotConflict()
		}
		encoded, err := encodeBackupConfigSnapshotValueChunkRecord(chunk)
		if err != nil {
			return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, err
		}
		key := backupConfigSnapshotValueChunkKey(entry.SnapshotID, entry.EntryOrdinal, chunk.ChunkOrdinal)
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded})
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: 0})
		if chunk.Plain != nil {
			_, _ = plainHash.Write(chunk.Plain.Content)
			plainLength += uint64(len(chunk.Plain.Content))
		}
		cursor.Record.StoredValueChainSHA256 = captureStoredChunkChain(
			"value",
			cursor.Record.StoredValueChainSHA256,
			key,
			encoded,
		)
	}
	if !entry.Secret &&
		(plainLength != entry.PlainValueLength || hex.EncodeToString(plainHash.Sum(nil)) != entry.PlainValueSHA256) {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, captureSnapshotConflict()
	}
	cursor.Record.EntryCount++
	cursor.Record.NextEntryOrdinal = cursor.Record.EntryCount
	cursor.Record.DescriptorChunkCount += uint64(len(descriptors))
	cursor.Record.ValueChunkCount += uint64(len(values))
	cursor.Record.PlainValueBytes += plainLength
	cursor.Record.UpdatedAt = time.Now().UTC()
	encodedCursor, err := EncodeBackupConfigSnapshotRecord(cursor.Record)
	if err != nil {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, err
	}
	mutations = append(
		mutations,
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   BackupConfigSnapshotKey(entry.SnapshotID),
			Value: encodedCursor,
		},
	)
	revision, err := repository.transactBounded(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[BackupConfigSnapshotRecord]{}, err
	}
	cursor.Revision, cursor.ReadRevision = revision, revision
	return cursor, nil
}

func (repository *CaptureSnapshotRepository) Seal(
	ctx context.Context,
	authority CaptureSnapshotWriteAuthority,
	expected etcdstore.Versioned[BackupConfigSnapshotRecord],
	evidence CaptureSnapshotEvidence,
) error {
	cursor, conditions, err := repository.readClaim(ctx, authority)
	if err != nil {
		return err
	}
	if authority.Expected == nil || authority.Expected.Content == nil {
		return captureSnapshotConflict()
	}
	canonicalAuthority, err := proto.MarshalOptions{Deterministic: true}.Marshal(authority.Expected)
	if err != nil {
		return captureSnapshotConflict()
	}
	defer clear(canonicalAuthority)
	if cursor.Revision != expected.Revision || cursor.Record.State != BackupConfigSnapshotBuilding ||
		cursor.Record.DescriptorChainSHA256 != expected.Record.DescriptorChainSHA256 ||
		cursor.Record.StoredValueChainSHA256 != expected.Record.StoredValueChainSHA256 ||
		evidence.EntryCount != authority.Expected.Content.EntryCount || evidence.MetadataProtoBytes != authority.Expected.MetadataProtoBytes ||
		evidence.ManifestSHA256 != hex.EncodeToString(authority.Expected.Content.ManifestSha256) ||
		evidence.SourceSizeBytes != authority.Expected.Content.SourceSizeBytes || !bytes.Equal(evidence.Authority, canonicalAuthority) ||
		evidence.SnapshotID != cursor.Record.SnapshotID || evidence.EnvironmentID != cursor.Record.EnvironmentID ||
		evidence.SourceID != cursor.Record.SourceID || evidence.ReadRevision != cursor.Record.ReadRevision ||
		uint64(
			evidence.EntryCount,
		) != cursor.Record.EntryCount || evidence.MetadataProtoBytes > backupconfig.MaxDurableMetadataBytes ||
		!validBackupConfigSHA256(evidence.ManifestSHA256) || !validBackupConfigSHA256(evidence.SourceSHA256) ||
		evidence.SourceSizeBytes < 2*backupconfig.TarBlockBytes || evidence.SourceSizeBytes > backupconfig.MaxSourceBytes ||
		len(evidence.Authority) == 0 || len(evidence.Authority) > maximumBackupConfigDurableRecordBytes {
		return captureSnapshotConflict()
	}
	cursor.Record.State = BackupConfigSnapshotSealed
	cursor.Record.StoredManifestSHA256 = evidence.ManifestSHA256
	cursor.Record.UpdatedAt = time.Now().UTC()
	cursorBytes, err := EncodeBackupConfigSnapshotRecord(cursor.Record)
	if err != nil {
		return err
	}
	defer clear(cursorBytes)
	evidenceBytes, err := recordcodec.Encode("backup-config-capture-evidence", evidence)
	if err != nil {
		return err
	}
	defer clear(evidenceBytes)
	key := captureSnapshotEvidenceKey(evidence.SnapshotID)
	conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: 0})
	_, err = repository.transactBounded(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: BackupConfigSnapshotKey(evidence.SnapshotID), Value: cursorBytes},
		{Type: etcdstore.MutationPut, Key: key, Value: evidenceBytes},
	})
	return err
}

func (repository *CaptureSnapshotRepository) ReadSealedEvidence(
	ctx context.Context,
	authority CaptureSnapshotWriteAuthority,
) (CaptureSnapshotEvidence, error) {
	snapshotID := authority.SnapshotID
	cursor, _, err := repository.readClaim(ctx, authority)
	if err != nil {
		return CaptureSnapshotEvidence{}, err
	}
	if cursor.Record.State != BackupConfigSnapshotSealed {
		return CaptureSnapshotEvidence{}, captureSnapshotConflict()
	}
	read, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{captureSnapshotEvidenceKey(snapshotID)}, Revision: cursor.ReadRevision},
	)
	if err != nil {
		return CaptureSnapshotEvidence{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return CaptureSnapshotEvidence{}, captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	evidence, err := recordcodec.Decode[CaptureSnapshotEvidence](read.Values[0].Value, "backup-config-capture-evidence")
	if err != nil || evidence.SnapshotID != snapshotID || evidence.EnvironmentID != cursor.Record.EnvironmentID ||
		!validBackupConfigSHA256(evidence.SourceSHA256) || evidence.SourceSizeBytes < 2*backupconfig.TarBlockBytes ||
		evidence.SourceSizeBytes > backupconfig.MaxSourceBytes || evidence.MetadataProtoBytes > backupconfig.MaxDurableMetadataBytes ||
		len(evidence.Authority) == 0 || len(evidence.Authority) > maximumBackupConfigDurableRecordBytes ||
		evidence.SourceID != cursor.Record.SourceID || evidence.ReadRevision != cursor.Record.ReadRevision ||
		uint64(
			evidence.EntryCount,
		) != cursor.Record.EntryCount || evidence.ManifestSHA256 != cursor.Record.StoredManifestSHA256 {
		return CaptureSnapshotEvidence{}, captureSnapshotConflict()
	}
	return evidence, nil
}

func (repository *CaptureSnapshotRepository) transactBounded(
	ctx context.Context,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (int64, error) {
	budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
	if err != nil {
		return 0, err
	}
	if budget.Bytes > MaximumBackupConfigBatchMutationBytes || budget.Operations > etcdstore.MaximumOperations {
		return 0, errs.New(errs.KindValidationFailed, "Config snapshot append exceeds its transaction limit")
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded || result.Revision <= 0 {
		return 0, captureSnapshotConflict()
	}
	return result.Revision, nil
}

func captureStoredChunkChain(kind, previous, key string, value []byte) string {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("groundplane.backup.config.stored-" + kind + "-chain.v1\x00"))
	prior, _ := hex.DecodeString(previous)
	if len(prior) == 0 {
		prior = make([]byte, sha256.Size)
	}
	_, _ = hasher.Write(prior)
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(key)))
	_, _ = hasher.Write(size[:])
	_, _ = hasher.Write([]byte(key))
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = hasher.Write(size[:])
	_, _ = hasher.Write(value)
	return hex.EncodeToString(hasher.Sum(nil))
}

func captureSnapshotConflict() error {
	return errs.New(errs.KindStateConflict, "Config snapshot ownership or committed cursor changed")
}
