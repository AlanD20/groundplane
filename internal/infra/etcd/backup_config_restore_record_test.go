package etcd

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a staged restore Entry must preserve exact artifact evidence
// without manufacturing old etcd revisions or value-generation identities.
func TestBackupConfigRestoreEntryRecordRoundTrip(t *testing.T) {
	now := testBackupConfigTime()
	record := BackupConfigRestoreEntryRecord{
		TaskID: ids.NewAt(ids.KindTask, now, 1), EntryOrdinal: 4,
		EntryID: ids.NewAt(ids.KindEnvEntry, now, 2), ValueGenerationID: ids.NewAt(ids.KindConfig, now, 3),
		Secret:           false,
		DescriptorLength: 27, DescriptorSHA256: testBackupConfigSHA256([]byte("descriptor")),
		DescriptorChunks: 1, PlainValueLength: 12,
		PlainValueSHA256: testBackupConfigSHA256([]byte("value")),
		ValueChunks:      1,
	}
	encoded, err := encodeBackupConfigRestoreEntryRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupConfigRestoreEntryRecord() error = %v", err)
	}
	restored, err := decodeBackupConfigRestoreEntryRecord(encoded)
	if err != nil {
		t.Fatalf("decodeBackupConfigRestoreEntryRecord() error = %v", err)
	}
	if restored != record {
		t.Fatalf("restored Config restore Entry = %#v, want %#v", restored, record)
	}
}

// Rationale: staged descriptor bytes must remain bounded and independently
// digest-verifiable before any active-generation switch is possible.
func TestBackupConfigRestoreDescriptorChunkRoundTrip(t *testing.T) {
	content := []byte("validated descriptor")
	record := BackupConfigRestoreDescriptorChunkRecord{
		TaskID: ids.NewAt(ids.KindTask, testBackupConfigTime(), 1), EntryOrdinal: 1,
		EntryID:           ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2),
		ValueGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 3), Secret: false,
		ChunkOrdinal: 0, Offset: 0, Length: uint32(len(content)),
		SHA256: testBackupConfigSHA256(content), Content: content,
	}
	encoded, err := encodeBackupConfigRestoreDescriptorChunkRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupConfigRestoreDescriptorChunkRecord() error = %v", err)
	}
	restored, err := decodeBackupConfigRestoreDescriptorChunkRecord(encoded)
	if err != nil {
		t.Fatalf("decodeBackupConfigRestoreDescriptorChunkRecord() error = %v", err)
	}
	if restored.TaskID != record.TaskID || restored.EntryOrdinal != record.EntryOrdinal ||
		restored.ChunkOrdinal != record.ChunkOrdinal || restored.Offset != record.Offset ||
		restored.Length != record.Length || restored.SHA256 != record.SHA256 ||
		!bytes.Equal(restored.Content, record.Content) {
		t.Fatalf("restored descriptor chunk = %#v, want %#v", restored, record)
	}
}

// Rationale: restore staging must use the same closed plain/protected union
// as backup capture and never retain a secret chunk as durable plaintext.
func TestBackupConfigRestoreProtectedValueChunkRoundTrip(t *testing.T) {
	plaintext := bytes.Repeat([]byte("restored-secret"), 3179)
	ciphertext := []byte("protected-restored-secret")
	record := BackupConfigRestoreValueChunkRecord{
		TaskID: ids.NewAt(ids.KindTask, testBackupConfigTime(), 1), EntryOrdinal: 1,
		EntryID:           ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2),
		ValueGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 3), Secret: true,
		Storage: BackupConfigChunkStorageControllerProtected,
		Protected: &BackupConfigProtectedChunkPayload{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextSHA256: testBackupConfigSHA256(ciphertext), Ciphertext: ciphertext,
		},
	}
	encoded, err := encodeBackupConfigRestoreValueChunkRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupConfigRestoreValueChunkRecord() error = %v", err)
	}
	plaintextDigest := []byte(testBackupConfigSHA256(plaintext))
	plaintextLength := []byte(strconv.Itoa(len(plaintext)))
	if bytes.Contains(encoded, plaintext) || bytes.Contains(encoded, plaintextDigest) ||
		bytes.Contains(encoded, plaintextLength) || bytes.Contains(encoded, []byte(`"plain"`)) {
		t.Fatalf("restore protected chunk encoded plaintext shape: %s", encoded)
	}
	restored, err := decodeBackupConfigRestoreValueChunkRecord(encoded)
	if err != nil {
		t.Fatalf("decodeBackupConfigRestoreValueChunkRecord() error = %v", err)
	}
	if restored.Plain != nil || restored.Protected == nil ||
		!bytes.Equal(restored.Protected.Ciphertext, ciphertext) {
		t.Fatalf("restored protected value chunk = %#v", restored)
	}
}

// Rationale: restore staging must fail closed before etcd can contain plaintext
// for an Entry whose artifact descriptor marks it Secret.
func TestBackupConfigRestoreValueChunkRejectsStorageOutsideEntryClass(t *testing.T) {
	content := []byte("restored-secret")
	now := testBackupConfigTime()
	tests := map[string]BackupConfigRestoreValueChunkRecord{
		"secret plaintext": {
			TaskID: ids.NewAt(ids.KindTask, now, 1), EntryID: ids.NewAt(ids.KindEnvEntry, now, 2),
			ValueGenerationID: ids.NewAt(ids.KindConfig, now, 3),
			Secret:            true, Storage: BackupConfigChunkStoragePlain,
			Plain: &BackupConfigPlainChunkPayload{
				ContentLength: uint32(len(content)), ContentSHA256: testBackupConfigSHA256(content), Content: content,
			},
		},
		"plain protected": {
			TaskID: ids.NewAt(ids.KindTask, now, 1), EntryID: ids.NewAt(ids.KindEnvEntry, now, 2),
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
			encoded, err := encodeBackupConfigRestoreValueChunkRecord(record)
			clear(encoded)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("encodeBackupConfigRestoreValueChunkRecord() error = %v, want validation", err)
			}
		})
	}
}

// Rationale: unknown transaction outcomes are recoverable only when every
// staged Entry preallocates the immutable value-generation identity it will create.
func TestBackupConfigRestoreEntryRequiresFreshValueGenerationIdentity(t *testing.T) {
	now := testBackupConfigTime()
	record := BackupConfigRestoreEntryRecord{
		TaskID: ids.NewAt(ids.KindTask, now, 1), EntryID: ids.NewAt(ids.KindEnvEntry, now, 2),
		DescriptorLength: 1, DescriptorSHA256: testBackupConfigSHA256([]byte("d")), DescriptorChunks: 1,
		PlainValueSHA256: testBackupConfigSHA256(nil),
	}
	if _, err := encodeBackupConfigRestoreEntryRecord(record); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("missing restore value generation error = %v, want validation", err)
	}
}

// Rationale: a staged Entry cannot claim impossible chunk counts or omit the
// final digest that authorizes canonical apply.
func TestBackupConfigRestoreEntryRejectsImpossibleChunkSummary(t *testing.T) {
	record := BackupConfigRestoreEntryRecord{
		TaskID:            ids.NewAt(ids.KindTask, testBackupConfigTime(), 1),
		EntryID:           ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2),
		ValueGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 3),
		DescriptorLength:  MaximumBackupConfigChunkPayloadBytes + 1,
		DescriptorSHA256:  testBackupConfigSHA256([]byte("descriptor")), DescriptorChunks: 1,
		PlainValueSHA256: testBackupConfigSHA256(nil),
	}
	if _, err := encodeBackupConfigRestoreEntryRecord(
		record,
	); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("impossible restore Entry error = %v, want validation", err)
	}
}

// Rationale: zero-padded runtime keys preserve artifact order while the two
// uniqueness fences reject duplicate Entry ids and normalized destinations.
func TestBackupConfigRestoreKeysAreCanonical(t *testing.T) {
	taskID := ids.NewAt(ids.KindTask, testBackupConfigTime(), 1)
	entryID := ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2)
	entrySuffix := "/00000000000000000009"
	if got := backupConfigRestoreEntryKey(taskID, 9); got !=
		"/v1/runtime/config-restore-entries/"+taskID+entrySuffix {
		t.Fatalf("restore Entry key = %q", got)
	}
	if got := backupConfigRestoreDescriptorChunkKey(taskID, 9, 4); got !=
		"/v1/runtime/config-restore-descriptors/"+taskID+entrySuffix+"/0000000004" {
		t.Fatalf("restore descriptor key = %q", got)
	}
	if got := backupConfigRestoreValueChunkKey(taskID, 9, 4); got !=
		"/v1/runtime/config-restore-values/"+taskID+entrySuffix+"/0000000004" {
		t.Fatalf("restore value key = %q", got)
	}
	if got := backupConfigRestoreEntryIdentityKey(taskID, entryID); got !=
		"/v1/runtime/config-restore-identities/"+taskID+"/id/"+entryID {
		t.Fatalf("restore Entry identity key = %q", got)
	}
	if got := backupConfigRestoreDestinationKey(taskID, "secrets/app token"); got !=
		"/v1/runtime/config-restore-identities/"+taskID+"/destination/~c2VjcmV0cy9hcHAgdG9rZW4" {
		t.Fatalf("restore destination key = %q", got)
	}
}

// Rationale: a value encoded for snapshot staging must not be accepted as a
// restore record even though its payload fields otherwise look identical.
func TestBackupConfigRestoreDecoderRejectsSnapshotEnvelope(t *testing.T) {
	content := []byte("plain")
	snapshot := BackupConfigSnapshotValueChunkRecord{
		SnapshotID:        ids.NewAt(ids.KindTask, testBackupConfigTime(), 1),
		EntryID:           ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2),
		ValueGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 3),
		Storage:           BackupConfigChunkStoragePlain,
		Plain: &BackupConfigPlainChunkPayload{
			ContentLength: uint32(len(content)), ContentSHA256: testBackupConfigSHA256(content), Content: content,
		},
	}
	encoded, err := encodeBackupConfigSnapshotValueChunkRecord(snapshot)
	if err != nil {
		t.Fatalf("encodeBackupConfigSnapshotValueChunkRecord() error = %v", err)
	}
	if _, err := decodeBackupConfigRestoreValueChunkRecord(encoded); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("decode snapshot as restore error = %v, want internal", err)
	}
}
