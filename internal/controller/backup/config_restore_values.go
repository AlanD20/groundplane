package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type configRestoreValueStore interface {
	StageConfigRestoreEntry(
		context.Context,
		etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord],
		uint32,
		entries.Record,
		uint64,
		string,
		entries.EntryValueGeneration,
	) (entries.EntryValueGeneration, error)
}

// StageValues runs only after the entire candidate passed preparation. Native
// receipts preserve the first ciphertext; replay independently decrypts and
// compares it with the selected archive before accepting that receipt.
func (publication *ConfigRestorePublication) StageValues(ctx context.Context,
	generation *ConfigRestoreGeneration, store configRestoreValueStore,
	protector *secretvalue.Protector, createdAt time.Time,
) error {
	if ctx == nil || publication == nil || generation == nil || store == nil || protector == nil ||
		createdAt.IsZero() ||
		publication.Projection.EnvironmentID != generation.owner.EnvironmentID ||
		publication.Projection.RevisionID != generation.owner.Transfer.Binding.TaskID ||
		publication.Projection.RenderGeneration != generation.owner.RenderGeneration ||
		len(publication.Projection.Entries) != int(generation.seal.Record.Completed.Content.EntryCount) {
		return configSnapshotGuardConflict()
	}
	return generation.VisitEntries(ctx, func(ordinal uint32, archived backupconfig.Entry, value []byte) error {
		if ordinal == 0 || int(ordinal) > len(publication.Projection.Entries) {
			return configSnapshotGuardConflict()
		}
		record := publication.Projection.Entries[ordinal-1]
		if record.Entry.ID != archived.ID || record.CurrentValueGenerationID != generation.owner.GenerationID ||
			record.Entry.Secret != archived.Secret {
			return configSnapshotGuardConflict()
		}
		prepared, err := sealRestoredEntryValue(ctx, protector, record, value, createdAt)
		if err != nil {
			return err
		}
		defer clearRestoredEntryValue(prepared)
		stored, err := store.StageConfigRestoreEntry(ctx, generation.Seal(), ordinal, record,
			archived.Value.SizeBytes, hex.EncodeToString(archived.Value.SHA256[:]), prepared)
		if err != nil {
			return err
		}
		defer clearRestoredEntryValue(stored)
		return verifyRestoredEntryValue(ctx, protector, stored, value, createdAt)
	})
}

func sealRestoredEntryValue(ctx context.Context, protector *secretvalue.Protector,
	record entries.Record, value []byte, createdAt time.Time,
) (entries.EntryValueGeneration, error) {
	if !record.Entry.Secret {
		digest := sha256.Sum256(value)
		return entries.EntryValueGeneration{Plain: &entryvalues.PlainGeneration{EnvironmentID: record.EnvironmentID,
			EntryID: record.Entry.ID, GenerationID: record.CurrentValueGenerationID, Content: append([]byte(nil), value...),
			PlaintextSHA256: hex.EncodeToString(digest[:]), CreatedAt: createdAt.UTC()}}, nil
	}
	envelope, err := protector.Seal(ctx, value)
	if err != nil {
		return entries.EntryValueGeneration{}, err
	}
	defer envelope.Clear()
	metadata := envelope.Metadata()
	return entries.EntryValueGeneration{Secret: &entryvalues.SecretGeneration{EnvironmentID: record.EnvironmentID,
		EntryID: record.Entry.ID, GenerationID: record.CurrentValueGenerationID, EnvelopeVersion: uint8(metadata.Version),
		Cipher: string(metadata.Cipher), DigestAlgorithm: string(metadata.Digest.Algorithm),
		CiphertextSHA256: metadata.Digest.Value, Ciphertext: envelope.Ciphertext(), CreatedAt: createdAt.UTC()}}, nil
}

func verifyRestoredEntryValue(ctx context.Context, protector *secretvalue.Protector,
	generation entries.EntryValueGeneration, selected []byte, createdAt time.Time,
) error {
	return openRestoredEntryValue(ctx, protector, generation, createdAt, func(value []byte) error {
		if !bytes.Equal(value, selected) {
			return configSnapshotGuardConflict()
		}
		return nil
	})
}

func openRestoredEntryValue(ctx context.Context, protector *secretvalue.Protector,
	generation entries.EntryValueGeneration, createdAt time.Time, consume func([]byte) error,
) error {
	if (generation.Plain == nil) == (generation.Secret == nil) {
		return configSnapshotGuardConflict()
	}
	if generation.Plain != nil {
		if !generation.Plain.CreatedAt.Equal(createdAt.UTC()) {
			return configSnapshotGuardConflict()
		}
		return consume(generation.Plain.Content)
	}
	stored := generation.Secret
	if !stored.CreatedAt.Equal(createdAt.UTC()) {
		return configSnapshotGuardConflict()
	}
	envelope, err := secretvalue.Restore(
		secretvalue.Metadata{Version: secretvalue.EnvelopeVersion(stored.EnvelopeVersion),
			Cipher: secretvalue.CipherSuite(
				stored.Cipher,
			), Digest: secretvalue.Digest{Algorithm: secretvalue.DigestAlgorithm(stored.DigestAlgorithm), Value: stored.CiphertextSHA256}},
		stored.Ciphertext,
	)
	if err != nil {
		return err
	}
	defer envelope.Clear()
	return protector.Open(ctx, envelope, consume)
}

func clearRestoredEntryValue(generation entries.EntryValueGeneration) {
	if generation.Plain != nil {
		clear(generation.Plain.Content)
	}
	if generation.Secret != nil {
		clear(generation.Secret.Ciphertext)
	}
}
