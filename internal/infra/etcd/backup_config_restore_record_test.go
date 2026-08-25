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
		RestoreGenerationID: ids.NewAt(ids.KindConfig, now, 1), EntryOrdinal: 4,
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
		RestoreGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 1), EntryOrdinal: 1,
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
	if restored.RestoreGenerationID != record.RestoreGenerationID || restored.EntryOrdinal != record.EntryOrdinal ||
		restored.EntryID != record.EntryID || restored.ValueGenerationID != record.ValueGenerationID ||
		restored.Secret != record.Secret || restored.ChunkOrdinal != record.ChunkOrdinal ||
		restored.Offset != record.Offset || restored.Length != record.Length ||
		restored.SHA256 != record.SHA256 ||
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
		RestoreGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 1), EntryOrdinal: 1,
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
	if restored.RestoreGenerationID != record.RestoreGenerationID || restored.EntryOrdinal != record.EntryOrdinal ||
		restored.EntryID != record.EntryID || restored.ValueGenerationID != record.ValueGenerationID ||
		restored.Secret != record.Secret || restored.ChunkOrdinal != record.ChunkOrdinal ||
		restored.Storage != record.Storage || restored.Plain != nil || restored.Protected == nil ||
		restored.Protected.EnvelopeVersion != record.Protected.EnvelopeVersion ||
		restored.Protected.Cipher != record.Protected.Cipher ||
		restored.Protected.DigestAlgorithm != record.Protected.DigestAlgorithm ||
		restored.Protected.CiphertextSHA256 != record.Protected.CiphertextSHA256 ||
		!bytes.Equal(restored.Protected.Ciphertext, record.Protected.Ciphertext) {
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
			RestoreGenerationID: ids.NewAt(ids.KindConfig, now, 1), EntryID: ids.NewAt(ids.KindEnvEntry, now, 2),
			ValueGenerationID: ids.NewAt(ids.KindConfig, now, 3),
			Secret:            true, Storage: BackupConfigChunkStoragePlain,
			Plain: &BackupConfigPlainChunkPayload{
				ContentLength: uint32(len(content)), ContentSHA256: testBackupConfigSHA256(content), Content: content,
			},
		},
		"plain protected": {
			RestoreGenerationID: ids.NewAt(ids.KindConfig, now, 1), EntryID: ids.NewAt(ids.KindEnvEntry, now, 2),
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
		RestoreGenerationID: ids.NewAt(ids.KindConfig, now, 1), EntryID: ids.NewAt(ids.KindEnvEntry, now, 2),
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
		RestoreGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 1),
		EntryID:             ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2),
		ValueGenerationID:   ids.NewAt(ids.KindConfig, testBackupConfigTime(), 3),
		DescriptorLength:    MaximumBackupConfigChunkPayloadBytes + 1,
		DescriptorSHA256:    testBackupConfigSHA256([]byte("descriptor")), DescriptorChunks: 1,
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
	restoreGenerationID := ids.NewAt(ids.KindConfig, testBackupConfigTime(), 1)
	entryID := ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2)
	entrySuffix := "/00000000000000000009"
	if got, err := backupConfigRestoreEntryKey(restoreGenerationID, 9); err != nil || got !=
		"/v1/runtime/config-restore-entries/"+restoreGenerationID+entrySuffix {
		t.Fatalf("restore Entry key = %q", got)
	}
	if got, err := backupConfigRestoreDescriptorChunkKey(restoreGenerationID, 9, 4); err != nil || got !=
		"/v1/runtime/config-restore-descriptors/"+restoreGenerationID+entrySuffix+"/0000000004" {
		t.Fatalf("restore descriptor key = %q", got)
	}
	if got, err := backupConfigRestoreValueChunkKey(restoreGenerationID, 9, 4); err != nil || got !=
		"/v1/runtime/config-restore-values/"+restoreGenerationID+entrySuffix+"/0000000004" {
		t.Fatalf("restore value key = %q", got)
	}
	if got, err := backupConfigRestoreEntryIdentityKey(restoreGenerationID, entryID); err != nil || got !=
		"/v1/runtime/config-restore-identities/"+restoreGenerationID+"/id/"+entryID {
		t.Fatalf("restore Entry identity key = %q", got)
	}
	if got, err := backupConfigRestoreDestinationKey(restoreGenerationID, "secrets/app token"); err != nil || got !=
		"/v1/runtime/config-restore-identities/"+restoreGenerationID+
			"/destination/~c2VjcmV0cy9hcHAgdG9rZW4" {
		t.Fatalf("restore destination key = %q", got)
	}
}

// Rationale: the private restore generation and each immutable per-Entry value
// generation are separate identities; aliasing them would make cleanup and
// activation authorities indistinguishable after retry.
func TestBackupConfigRestoreRecordsRejectRestoreGenerationAsValueGeneration(t *testing.T) {
	now := testBackupConfigTime()
	restoreGenerationID := ids.NewAt(ids.KindConfig, now, 1)
	entryID := ids.NewAt(ids.KindEnvEntry, now, 2)
	content := []byte("value")
	tests := map[string]func() error{
		"Entry": func() error {
			_, err := encodeBackupConfigRestoreEntryRecord(BackupConfigRestoreEntryRecord{
				RestoreGenerationID: restoreGenerationID, EntryID: entryID,
				ValueGenerationID: restoreGenerationID,
				DescriptorLength:  1, DescriptorSHA256: testBackupConfigSHA256([]byte("d")),
				DescriptorChunks: 1, PlainValueSHA256: testBackupConfigSHA256(nil),
			})
			return err
		},
		"descriptor": func() error {
			_, err := encodeBackupConfigRestoreDescriptorChunkRecord(BackupConfigRestoreDescriptorChunkRecord{
				RestoreGenerationID: restoreGenerationID, EntryID: entryID,
				ValueGenerationID: restoreGenerationID,
				Length:            uint32(len(content)), SHA256: testBackupConfigSHA256(content), Content: content,
			})
			return err
		},
		"value": func() error {
			_, err := encodeBackupConfigRestoreValueChunkRecord(BackupConfigRestoreValueChunkRecord{
				RestoreGenerationID: restoreGenerationID, EntryID: entryID,
				ValueGenerationID: restoreGenerationID, Storage: BackupConfigChunkStoragePlain,
				Plain: &BackupConfigPlainChunkPayload{
					ContentLength: uint32(len(content)), ContentSHA256: testBackupConfigSHA256(content),
					Content: content,
				},
			})
			return err
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			if err := run(); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("encode colliding restore identity error = %v, want validation", err)
			}
		})
	}
}

// Rationale: exact record replay must remain valid, while a restore batch may
// not reuse an ordinal, Entry id, normalized destination, or value-generation
// identity for a different staged Entry.
func TestBackupConfigRestoreBatchIdentitiesPreserveExactReplayAndRejectDuplicates(t *testing.T) {
	now := testBackupConfigTime()
	restoreGenerationID := ids.NewAt(ids.KindConfig, now, 1)
	first := BackupConfigRestoreEntryRecord{
		RestoreGenerationID: restoreGenerationID, EntryOrdinal: 1,
		EntryID:           ids.NewAt(ids.KindEnvEntry, now, 2),
		ValueGenerationID: ids.NewAt(ids.KindConfig, now, 3),
		DescriptorLength:  1, DescriptorSHA256: testBackupConfigSHA256([]byte("d")), DescriptorChunks: 1,
		PlainValueSHA256: testBackupConfigSHA256(nil),
	}
	replayValue, err := encodeBackupConfigRestoreEntryRecord(first)
	if err != nil {
		t.Fatalf("encodeBackupConfigRestoreEntryRecord() error = %v", err)
	}
	replay, err := decodeBackupConfigRestoreEntryRecord(replayValue)
	if err != nil || replay != first {
		t.Fatalf("decode exact replay = %#v, %v, want %#v", replay, err, first)
	}
	if err := validateBackupConfigRestoreBatchIdentities(
		restoreGenerationID,
		[]BackupConfigRestoreEntryRecord{replay},
		[]string{"env/API_TOKEN"},
	); err != nil {
		t.Fatalf("validate exact replay identities error = %v", err)
	}
	second := BackupConfigRestoreEntryRecord{
		RestoreGenerationID: restoreGenerationID, EntryOrdinal: 2,
		EntryID:           ids.NewAt(ids.KindEnvEntry, now, 4),
		ValueGenerationID: ids.NewAt(ids.KindConfig, now, 5),
	}
	tests := map[string]struct {
		mutate      func(*BackupConfigRestoreEntryRecord)
		destination string
	}{
		"ordinal": {
			mutate:      func(record *BackupConfigRestoreEntryRecord) { record.EntryOrdinal = first.EntryOrdinal },
			destination: "env/SECOND",
		},
		"Entry id": {
			mutate:      func(record *BackupConfigRestoreEntryRecord) { record.EntryID = first.EntryID },
			destination: "env/SECOND",
		},
		"destination": {
			mutate:      func(*BackupConfigRestoreEntryRecord) {},
			destination: "env/API_TOKEN",
		},
		"value generation": {
			mutate: func(record *BackupConfigRestoreEntryRecord) {
				record.ValueGenerationID = first.ValueGenerationID
			},
			destination: "env/SECOND",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := second
			test.mutate(&candidate)
			if err := validateBackupConfigRestoreBatchIdentities(
				restoreGenerationID,
				[]BackupConfigRestoreEntryRecord{first, candidate},
				[]string{"env/API_TOKEN", test.destination},
			); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("duplicate %s error = %v, want validation", name, err)
			}
		})
	}
}

// Rationale: a persisted record that aliases restore and value generations is
// corrupt durable state, even if the envelope and both cfg_ identifiers are
// otherwise syntactically valid.
func TestBackupConfigRestoreDecoderRejectsCollidingGenerationIdentities(t *testing.T) {
	now := testBackupConfigTime()
	restoreGenerationID := ids.NewAt(ids.KindConfig, now, 1)
	record := BackupConfigRestoreEntryRecord{
		RestoreGenerationID: restoreGenerationID, EntryOrdinal: 1,
		EntryID: ids.NewAt(ids.KindEnvEntry, now, 2), ValueGenerationID: ids.NewAt(ids.KindConfig, now, 3),
		DescriptorLength: 1, DescriptorSHA256: testBackupConfigSHA256([]byte("d")), DescriptorChunks: 1,
		PlainValueSHA256: testBackupConfigSHA256(nil),
	}
	encoded, err := encodeBackupConfigRestoreEntryRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupConfigRestoreEntryRecord() error = %v", err)
	}
	malformed := bytes.Replace(
		encoded,
		[]byte(record.ValueGenerationID),
		[]byte(record.RestoreGenerationID),
		1,
	)
	if _, err := decodeBackupConfigRestoreEntryRecord(malformed); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("decode colliding generation identities error = %v, want internal", err)
	}
}

// Rationale: the private generation is a Controller-owned config identity,
// never an execution-attempt Task identity that would change on retry.
func TestBackupConfigRestoreRejectsTaskIdentityAsGeneration(t *testing.T) {
	taskID := ids.NewAt(ids.KindTask, testBackupConfigTime(), 1)
	if _, err := backupConfigRestoreEntryKey(taskID, 0); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Task-keyed restore Entry error = %v, want validation", err)
	}
	record := BackupConfigRestoreEntryRecord{
		RestoreGenerationID: taskID,
		EntryID:             ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2),
		ValueGenerationID:   ids.NewAt(ids.KindConfig, testBackupConfigTime(), 3),
		DescriptorLength:    1,
		DescriptorSHA256:    testBackupConfigSHA256([]byte("d")),
		DescriptorChunks:    1,
		PlainValueSHA256:    testBackupConfigSHA256(nil),
	}
	if _, err := encodeBackupConfigRestoreEntryRecord(record); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("Task-owned restore generation error = %v, want validation", err)
	}
}

// Rationale: retry changes the current Task owner only on bounded restore
// authority. Unbounded staged children must retain one generation identity and
// contain no attempt-scoped field that would require rewriting them.
func TestBackupConfigRestoreStagingExcludesAttemptOwnership(t *testing.T) {
	record := BackupConfigRestoreEntryRecord{
		RestoreGenerationID: ids.NewAt(ids.KindConfig, testBackupConfigTime(), 1),
		EntryID:             ids.NewAt(ids.KindEnvEntry, testBackupConfigTime(), 2),
		ValueGenerationID:   ids.NewAt(ids.KindConfig, testBackupConfigTime(), 3),
		DescriptorLength:    1,
		DescriptorSHA256:    testBackupConfigSHA256([]byte("d")),
		DescriptorChunks:    1,
		PlainValueSHA256:    testBackupConfigSHA256(nil),
	}
	encoded, err := encodeBackupConfigRestoreEntryRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupConfigRestoreEntryRecord() error = %v", err)
	}
	if bytes.Contains(encoded, []byte(`"task_id"`)) ||
		!bytes.Contains(encoded, []byte(`"restore_generation_id"`)) {
		t.Fatalf("restore staging ownership shape = %s", encoded)
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
