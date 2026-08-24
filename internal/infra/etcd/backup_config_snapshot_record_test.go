package etcd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the accepted bounded transfer contract must remain one explicit
// shared limit rather than diverging between snapshot and restore writers.
func TestBackupConfigChunkBoundsAreLocked(t *testing.T) {
	if MaximumBackupConfigChunkPayloadBytes != 48<<10 ||
		MaximumBackupConfigProtectedChunkCiphertextBytes != 64<<10 ||
		MaximumBackupConfigChunksPerTransaction != 12 ||
		MaximumBackupConfigBatchMutationBytes != 768<<10 {
		t.Fatal("Backup config chunk bounds changed")
	}
}

// Rationale: restart and retry must recover the exact fixed-revision capture
// cursor, including rolling and final digest evidence.
func TestBackupConfigSnapshotRecordRoundTrip(t *testing.T) {
	record := testBackupConfigSnapshotRecord()
	encoded, err := encodeBackupConfigSnapshotRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupConfigSnapshotRecord() error = %v", err)
	}
	restored, err := decodeBackupConfigSnapshotRecord(encoded)
	if err != nil {
		t.Fatalf("decodeBackupConfigSnapshotRecord() error = %v", err)
	}
	if restored != record {
		t.Fatalf("restored snapshot = %#v, want %#v", restored, record)
	}
}

// Rationale: empty Environment config is a valid sealed artifact, but it
// still requires a final manifest digest and no manufactured chain chunks.
func TestBackupConfigSnapshotRecordAcceptsEmptySealedSnapshot(t *testing.T) {
	record := testBackupConfigSnapshotRecord()
	record.NextEntryOrdinal = 0
	record.EntryCount = 0
	record.DescriptorChunkCount = 0
	record.ValueChunkCount = 0
	record.PlainValueBytes = 0
	record.DescriptorChainSHA256 = ""
	record.StoredValueChainSHA256 = ""
	if _, err := encodeBackupConfigSnapshotRecord(record); err != nil {
		t.Fatalf("encodeBackupConfigSnapshotRecord(empty) error = %v", err)
	}
}

// Rationale: state-specific progress must fail closed so a partial capture
// cannot be mistaken for a sealed immutable snapshot after restart.
func TestBackupConfigSnapshotRecordRejectsInvalidStateEvidence(t *testing.T) {
	tests := map[string]func(*BackupConfigSnapshotRecord){
		"uninitialized progress": func(record *BackupConfigSnapshotRecord) {
			record.State = BackupConfigSnapshotUninitialized
		},
		"building manifest": func(record *BackupConfigSnapshotRecord) {
			record.State = BackupConfigSnapshotBuilding
		},
		"sealed missing manifest": func(record *BackupConfigSnapshotRecord) {
			record.StoredManifestSHA256 = ""
		},
		"cursor count mismatch": func(record *BackupConfigSnapshotRecord) {
			record.NextEntryOrdinal++
		},
		"missing descriptor chain": func(record *BackupConfigSnapshotRecord) {
			record.DescriptorChainSHA256 = ""
		},
		"uppercase digest": func(record *BackupConfigSnapshotRecord) {
			record.StoredManifestSHA256 =
				"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			record := testBackupConfigSnapshotRecord()
			mutate(&record)
			if _, err := encodeBackupConfigSnapshotRecord(
				record,
			); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("encodeBackupConfigSnapshotRecord() error = %v, want validation", err)
			}
		})
	}
}

// Rationale: each finalized Entry must pin both etcd revisions and a
// possible chunk layout whose digests survive a restart exactly.
func TestBackupConfigSnapshotEntryRecordRoundTrip(t *testing.T) {
	now := testBackupConfigTime()
	record := BackupConfigSnapshotEntryRecord{
		SnapshotID: ids.NewAt(ids.KindTask, now, 1), EntryOrdinal: 7,
		EntryID: ids.NewAt(ids.KindEnvEntry, now, 2), EntryRevision: 41,
		ValueGenerationID: ids.NewAt(ids.KindConfig, now, 3), ValueGenerationRevision: 42,
		Secret: true, DescriptorLength: 20, DescriptorSHA256: testBackupConfigSHA256([]byte("descriptor")),
		DescriptorChunks: 1, ValueChunks: 1,
	}
	encoded, err := encodeBackupConfigSnapshotEntryRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupConfigSnapshotEntryRecord() error = %v", err)
	}
	restored, err := decodeBackupConfigSnapshotEntryRecord(encoded)
	if err != nil {
		t.Fatalf("decodeBackupConfigSnapshotEntryRecord() error = %v", err)
	}
	if restored != record {
		t.Fatalf("restored snapshot Entry = %#v, want %#v", restored, record)
	}
}

// Rationale: chunk records are independently restartable evidence and must
// reject a declared digest or length that does not match their bytes.
func TestBackupConfigSnapshotDescriptorChunkRoundTripAndValidation(t *testing.T) {
	content := []byte("canonical descriptor bytes")
	record := BackupConfigSnapshotDescriptorChunkRecord{
		SnapshotID: ids.NewAt(ids.KindTask, testBackupConfigTime(), 1), EntryOrdinal: 2,
		EntryID:           ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2),
		ValueGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 3), Secret: false,
		ChunkOrdinal: 3, Offset: 3 * MaximumBackupConfigChunkPayloadBytes, Length: uint32(len(content)),
		SHA256: testBackupConfigSHA256(content), Content: content,
	}
	encoded, err := encodeBackupConfigSnapshotDescriptorChunkRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupConfigSnapshotDescriptorChunkRecord() error = %v", err)
	}
	restored, err := decodeBackupConfigSnapshotDescriptorChunkRecord(encoded)
	if err != nil {
		t.Fatalf("decodeBackupConfigSnapshotDescriptorChunkRecord() error = %v", err)
	}
	if restored.SnapshotID != record.SnapshotID || restored.EntryOrdinal != record.EntryOrdinal ||
		restored.ChunkOrdinal != record.ChunkOrdinal || restored.Offset != record.Offset ||
		restored.Length != record.Length || restored.SHA256 != record.SHA256 ||
		!bytes.Equal(restored.Content, record.Content) {
		t.Fatalf("restored descriptor chunk = %#v, want %#v", restored, record)
	}
	record.SHA256 = testBackupConfigSHA256([]byte("different"))
	if _, err := encodeBackupConfigSnapshotDescriptorChunkRecord(
		record,
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("mismatched descriptor digest error = %v, want validation", err)
	}
}

// Rationale: a secret Config chunk may persist only Controller ciphertext;
// the plaintext union member and literal secret bytes must remain absent.
func TestBackupConfigSnapshotProtectedChunkNeverPersistsPlaintext(t *testing.T) {
	plaintext := bytes.Repeat([]byte("literal-secret-value"), 2117)
	ciphertext := []byte("opaque-controller-ciphertext")
	record := BackupConfigSnapshotValueChunkRecord{
		SnapshotID: ids.NewAt(ids.KindTask, testBackupConfigTime(), 1), EntryOrdinal: 2,
		EntryID:           ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2),
		ValueGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 3), Secret: true, ChunkOrdinal: 0,
		Storage: BackupConfigChunkStorageControllerProtected,
		Protected: &BackupConfigProtectedChunkPayload{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextSHA256: testBackupConfigSHA256(ciphertext), Ciphertext: ciphertext,
		},
	}
	encoded, err := encodeBackupConfigSnapshotValueChunkRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupConfigSnapshotValueChunkRecord() error = %v", err)
	}
	plaintextDigest := []byte(testBackupConfigSHA256(plaintext))
	plaintextLength := []byte(strconv.Itoa(len(plaintext)))
	if bytes.Contains(encoded, plaintext) || bytes.Contains(encoded, plaintextDigest) ||
		bytes.Contains(encoded, plaintextLength) || bytes.Contains(encoded, []byte(`"plain"`)) {
		t.Fatalf("protected chunk encoded plaintext shape: %s", encoded)
	}
	restored, err := decodeBackupConfigSnapshotValueChunkRecord(encoded)
	if err != nil {
		t.Fatalf("decodeBackupConfigSnapshotValueChunkRecord() error = %v", err)
	}
	if restored.Plain != nil || restored.Protected == nil ||
		!bytes.Equal(restored.Protected.Ciphertext, ciphertext) {
		t.Fatalf("restored protected chunk = %#v", restored)
	}
}

// Rationale: the tagged value union must not permit fallback from a claimed
// protected chunk to durable plaintext or accept two simultaneous payloads.
func TestBackupConfigSnapshotValueChunkRejectsConfusedStorageShape(t *testing.T) {
	content := []byte("value")
	record := BackupConfigSnapshotValueChunkRecord{
		SnapshotID:        ids.NewAt(ids.KindTask, testBackupConfigTime(), 1),
		EntryID:           ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2),
		ValueGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 3),
		Storage:           BackupConfigChunkStoragePlain,
		Plain: &BackupConfigPlainChunkPayload{
			ContentLength: uint32(len(content)), ContentSHA256: testBackupConfigSHA256(content), Content: content,
		},
	}
	record.Protected = &BackupConfigProtectedChunkPayload{}
	if _, err := encodeBackupConfigSnapshotValueChunkRecord(
		record,
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("confused value chunk error = %v, want validation", err)
	}
}

// Rationale: the durable chunk must carry the owning Entry storage class so a
// writer cannot persist a Secret Entry as plaintext or a plain Entry as opaque ciphertext.
func TestBackupConfigSnapshotValueChunkRejectsStorageOutsideEntryClass(t *testing.T) {
	content := []byte("must-not-be-stored-for-a-secret")
	now := testBackupConfigTime()
	tests := map[string]BackupConfigSnapshotValueChunkRecord{
		"secret plaintext": {
			SnapshotID: ids.NewAt(ids.KindTask, now, 1), EntryID: ids.NewAt(ids.KindEnvEntry, now, 2),
			ValueGenerationID: ids.NewAt(ids.KindConfig, now, 3),
			Secret:            true, Storage: BackupConfigChunkStoragePlain,
			Plain: &BackupConfigPlainChunkPayload{
				ContentLength: uint32(len(content)), ContentSHA256: testBackupConfigSHA256(content), Content: content,
			},
		},
		"plain protected": {
			SnapshotID: ids.NewAt(ids.KindTask, now, 1), EntryID: ids.NewAt(ids.KindEnvEntry, now, 2),
			ValueGenerationID: ids.NewAt(ids.KindConfig, now, 3),
			Storage:           BackupConfigChunkStorageControllerProtected,
			Protected: &BackupConfigProtectedChunkPayload{
				EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
				CiphertextSHA256: testBackupConfigSHA256(content), Ciphertext: content,
			},
		},
	}
	for name, record := range tests {
		t.Run(name, func(t *testing.T) {
			encoded, err := encodeBackupConfigSnapshotValueChunkRecord(record)
			clear(encoded)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("encodeBackupConfigSnapshotValueChunkRecord() error = %v, want validation", err)
			}
		})
	}
}

// Rationale: independently stored chunks must bind their owning Entry and use
// the one canonical fixed-size coordinate system used to reconstruct ordered content.
func TestBackupConfigSnapshotChunksRejectInvalidIdentityAndCoordinates(t *testing.T) {
	content := []byte("chunk")
	now := testBackupConfigTime()
	descriptor := BackupConfigSnapshotDescriptorChunkRecord{
		SnapshotID: ids.NewAt(ids.KindTask, now, 1), EntryID: ids.NewAt(ids.KindEnvEntry, now, 2),
		ValueGenerationID: ids.NewAt(ids.KindConfig, now, 3),
		ChunkOrdinal:      1, Offset: 0, Length: uint32(len(content)),
		SHA256: testBackupConfigSHA256(content), Content: content,
	}
	if _, err := encodeBackupConfigSnapshotDescriptorChunkRecord(descriptor); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("descriptor coordinate error = %v, want validation", err)
	}

	plain := BackupConfigSnapshotValueChunkRecord{
		SnapshotID: ids.NewAt(ids.KindTask, now, 1), EntryID: "ev_invalid", ChunkOrdinal: 1,
		ValueGenerationID: ids.NewAt(ids.KindConfig, now, 3),
		Storage:           BackupConfigChunkStoragePlain,
		Plain: &BackupConfigPlainChunkPayload{
			Offset: 0, ContentLength: uint32(len(content)), ContentSHA256: testBackupConfigSHA256(content),
			Content: content,
		},
	}
	if _, err := encodeBackupConfigSnapshotValueChunkRecord(plain); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("plain chunk identity/coordinate error = %v, want validation", err)
	}
}

// Rationale: an empty fixed-revision capture cannot truthfully reference child
// chunks, and canonical chunk counts admit only one deterministic partition.
func TestBackupConfigSnapshotRecordRejectsManufacturedProgress(t *testing.T) {
	tests := map[string]func(*BackupConfigSnapshotRecord){
		"empty descriptor chunks": func(record *BackupConfigSnapshotRecord) {
			record.NextEntryOrdinal = 0
			record.EntryCount = 0
			record.DescriptorChunkCount = 1
			record.ValueChunkCount = 0
			record.PlainValueBytes = 0
			record.StoredValueChainSHA256 = ""
		},
		"empty value chunks": func(record *BackupConfigSnapshotRecord) {
			record.NextEntryOrdinal = 0
			record.EntryCount = 0
			record.DescriptorChunkCount = 0
			record.DescriptorChainSHA256 = ""
			record.ValueChunkCount = 1
			record.PlainValueBytes = 0
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			record := testBackupConfigSnapshotRecord()
			mutate(&record)
			if _, err := encodeBackupConfigSnapshotRecord(record); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("encodeBackupConfigSnapshotRecord() error = %v, want validation", err)
			}
		})
	}
}

// Rationale: fixed-width keys preserve bytewise capture order and bind the
// owning Task to its one retry-stable snapshot in both directions.
func TestBackupConfigSnapshotKeysAreCanonical(t *testing.T) {
	taskID := ids.NewAt(ids.KindTask, testBackupConfigTime(), 1)
	if got := backupConfigSnapshotKey(taskID); got != "/v1/runtime/backup-config-snapshots/"+taskID {
		t.Fatalf("snapshot key = %q", got)
	}
	entrySuffix := "/00000000000000000007"
	if got := backupConfigSnapshotEntryKey(taskID, 7); got !=
		"/v1/runtime/backup-config-snapshot-entries/"+taskID+entrySuffix {
		t.Fatalf("snapshot Entry key = %q", got)
	}
	if got := backupConfigSnapshotDescriptorChunkKey(taskID, 7, 3); got !=
		"/v1/runtime/backup-config-snapshot-descriptors/"+taskID+entrySuffix+"/0000000003" {
		t.Fatalf("snapshot descriptor key = %q", got)
	}
	if got := backupConfigSnapshotValueChunkKey(taskID, 7, 3); got !=
		"/v1/runtime/backup-config-snapshot-values/"+taskID+entrySuffix+"/0000000003" {
		t.Fatalf("snapshot value key = %q", got)
	}
	if low, high := backupConfigChunkOrdinal(
		3,
	), backupConfigChunkOrdinal(
		^uint32(0),
	); len(high) != 10 || low >= high ||
		high != "4294967295" {
		t.Fatalf("chunk ordinal ordering low=%q high=%q", low, high)
	}
	if got := backupConfigSnapshotTaskReferenceKey(taskID, taskID); got !=
		"/v1/indexes/backup-config-snapshots/by-task/"+taskID+"/"+taskID {
		t.Fatalf("snapshot by-Task key = %q", got)
	}
	if got := backupConfigSnapshotReferenceTaskKey(taskID, taskID); got !=
		"/v1/indexes/backup-config-snapshots/by-snapshot/"+taskID+"/"+taskID {
		t.Fatalf("snapshot by-snapshot key = %q", got)
	}
}

// Rationale: malformed durable envelopes must be internal corruption rather
// than decoded defaults or operator-controlled validation failures.
func TestBackupConfigSnapshotDecoderRejectsDuplicateEnvelopeField(t *testing.T) {
	encoded, err := encodeBackupConfigSnapshotRecord(testBackupConfigSnapshotRecord())
	if err != nil {
		t.Fatalf("encodeBackupConfigSnapshotRecord() error = %v", err)
	}
	corrupt := bytes.Replace(encoded, []byte(`"schema":1`), []byte(`"schema":1,"schema":1`), 1)
	if _, err := decodeBackupConfigSnapshotRecord(corrupt); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decode duplicate envelope error = %v, want internal", err)
	}
}

func testBackupConfigSnapshotRecord() BackupConfigSnapshotRecord {
	now := testBackupConfigTime()
	return BackupConfigSnapshotRecord{
		SnapshotID: ids.NewAt(ids.KindTask, now, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2),
		SourceID: ids.NewAt(ids.KindBackupSource, now, 3), State: BackupConfigSnapshotSealed,
		ReadRevision: 100, NextEntryOrdinal: 2, EntryCount: 2, DescriptorChunkCount: 2,
		ValueChunkCount: 2, PlainValueBytes: 16,
		DescriptorChainSHA256:  testBackupConfigSHA256([]byte("descriptor-chain")),
		StoredValueChainSHA256: testBackupConfigSHA256([]byte("stored-value-chain")),
		StoredManifestSHA256:   testBackupConfigSHA256([]byte("stored-manifest")),
		CreatedAt:              now, UpdatedAt: now.Add(time.Second),
	}
}

func testBackupConfigTime() time.Time {
	return time.Date(2026, 8, 24, 12, 0, 0, 123456789, time.UTC)
}

func testBackupConfigSHA256(content []byte) string {
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}
