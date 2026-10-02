package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CapturePublished verifies fixed-revision preparation against the persisted
// plan, then appends ciphertext-safe chunks to its already claimed snapshot.
// Nothing here publishes a Recovery Point or authorizes a live restore target.
func (producer *ConfigSnapshotProducer) CapturePublished(
	ctx context.Context,
	input backupplanning.BackupConfigSnapshotInput,
	expected *agentpb.BackupConfigCaptureAuthority,
) error {
	writeAuthority := backupconfiguration.CaptureSnapshotWriteAuthority{
		TaskID:        input.TaskID,
		SnapshotID:    input.SnapshotID,
		EnvironmentID: input.EnvironmentID,
		SourceID:      input.SourceID,
		ReadRevision:  input.ReadRevision,
		Expected:      proto.CloneOf(expected),
	}
	cursor, err := producer.snapshots.ReadCursor(ctx, writeAuthority)
	if err != nil {
		return err
	}
	if cursor.Record.EnvironmentID != input.EnvironmentID || cursor.Record.SourceID != input.SourceID ||
		cursor.Record.ReadRevision != input.ReadRevision {
		return configSnapshotInvalid()
	}
	if cursor.Record.State == backupconfiguration.BackupConfigSnapshotSealed {
		evidence, err := producer.snapshots.ReadSealedEvidence(ctx, writeAuthority)
		if err != nil {
			return err
		}
		stored := &agentpb.BackupConfigCaptureAuthority{}
		if err := proto.Unmarshal(evidence.Authority, stored); err != nil ||
			len(stored.ProtoReflect().GetUnknown()) != 0 ||
			!configSnapshotSameAuthority(stored, expected) {
			return configSnapshotInvalid()
		}
		return nil
	}
	prepared, err := producer.prepareConfigSnapshot(ctx, input)
	if err != nil {
		return err
	}
	defer prepared.clear()
	if !configSnapshotSameAuthority(prepared.authority, expected) ||
		cursor.Record.EntryCount > uint64(len(prepared.sources)) {
		return configSnapshotInvalid()
	}
	for index := int(cursor.Record.EntryCount); index < len(prepared.sources); index++ {
		source, entry, descriptor := prepared.sources[index], prepared.entries[index], prepared.metadata[index].CanonicalEntry
		descriptorSHA := sha256.Sum256(descriptor)
		entryRecord := backupconfiguration.BackupConfigSnapshotEntryRecord{
			SnapshotID:              input.SnapshotID,
			EntryOrdinal:            uint64(index),
			EntryID:                 entry.ID,
			EntryRevision:           source.EntryRevision,
			ValueGenerationID:       source.Record.CurrentValueGenerationID,
			ValueGenerationRevision: source.ValueGenerationRevision,
			Secret:                  entry.Secret,
			DescriptorLength: uint64(
				len(descriptor),
			),
			DescriptorSHA256: hex.EncodeToString(descriptorSHA[:]),
			DescriptorChunks: 1,
		}
		descriptorRecord := backupconfiguration.BackupConfigSnapshotDescriptorChunkRecord{
			SnapshotID:        input.SnapshotID,
			EntryOrdinal:      uint64(index),
			EntryID:           entry.ID,
			ValueGenerationID: source.Record.CurrentValueGenerationID,
			Secret:            entry.Secret,
			Length:            uint32(len(descriptor)),
			SHA256:            entryRecord.DescriptorSHA256,
			Content:           descriptor,
		}
		var chunks []backupconfiguration.BackupConfigSnapshotValueChunkRecord
		err = producer.withConfigSnapshotValue(ctx, prepared.reader, source, func(value []byte) error {
			if uint64(len(value)) != entry.Value.SizeBytes || sha256.Sum256(value) != entry.Value.SHA256 {
				return configSnapshotInvalid()
			}
			var err error
			chunks, err = producer.configSnapshotChunks(ctx, input.SnapshotID, uint64(index), source, value)
			return err
		})
		if err != nil {
			clearConfigSnapshotChunks(chunks)
			return err
		}
		entryRecord.ValueChunks = uint32(len(chunks))
		if !entry.Secret {
			entryRecord.PlainValueLength = entry.Value.SizeBytes
			entryRecord.PlainValueSHA256 = hex.EncodeToString(entry.Value.SHA256[:])
		}
		cursor, err = producer.snapshots.AppendEntry(ctx, writeAuthority, cursor, entryRecord,
			[]backupconfiguration.BackupConfigSnapshotDescriptorChunkRecord{descriptorRecord}, chunks)
		clearConfigSnapshotChunks(chunks)
		if err != nil {
			return err
		}
	}
	canonicalAuthority, err := proto.MarshalOptions{Deterministic: true}.Marshal(prepared.authority)
	if err != nil {
		return err
	}
	defer clear(canonicalAuthority)
	return producer.snapshots.Seal(
		ctx,
		writeAuthority,
		cursor,
		backupconfiguration.CaptureSnapshotEvidence{SnapshotID: input.SnapshotID,
			EnvironmentID: input.EnvironmentID, SourceID: input.SourceID, ReadRevision: input.ReadRevision,
			EntryCount: expected.Content.EntryCount, MetadataProtoBytes: expected.MetadataProtoBytes,
			ManifestSHA256: hex.EncodeToString(
				expected.Content.ManifestSha256,
			), SourceSizeBytes: expected.Content.SourceSizeBytes,
			SourceSHA256: hex.EncodeToString(prepared.sourceSHA256[:]), Authority: canonicalAuthority},
	)
}

func (producer *ConfigSnapshotProducer) configSnapshotChunks(
	ctx context.Context,
	snapshotID string,
	ordinal uint64,
	source backupconfiguration.CaptureSourceEntry,
	value []byte,
) (_ []backupconfiguration.BackupConfigSnapshotValueChunkRecord, resultErr error) {
	var chunks []backupconfiguration.BackupConfigSnapshotValueChunkRecord
	defer func() {
		if resultErr != nil {
			clearConfigSnapshotChunks(chunks)
		}
	}()
	for offset := 0; offset < len(value) || offset == 0 && source.Record.Entry.Secret; {
		end := offset + backupconfiguration.MaximumBackupConfigChunkPayloadBytes
		if end > len(value) {
			end = len(value)
		}
		content := value[offset:end]
		digest := sha256.Sum256(content)
		payload := backupconfiguration.BackupConfigPlainChunkPayload{
			Offset:        uint64(offset),
			ContentLength: uint32(len(content)),
			ContentSHA256: hex.EncodeToString(digest[:]),
			Content:       content,
		}
		chunk := backupconfiguration.BackupConfigSnapshotValueChunkRecord{SnapshotID: snapshotID, EntryOrdinal: ordinal,
			EntryID: source.Record.Entry.ID, ValueGenerationID: source.Record.CurrentValueGenerationID,
			Secret: source.Record.Entry.Secret, ChunkOrdinal: uint32(len(chunks))}
		if source.Record.Entry.Secret {
			// Protect the raw selected bytes, including an empty selected value.
			// The atomic stored-chunk chain binds the envelope to its Entry and
			// ordinal; canonical metadata binds complete value length and hash.
			envelope, err := producer.protector.Seal(ctx, content)
			if err != nil {
				return nil, err
			}
			metadata := envelope.Metadata()
			ciphertext := envelope.Ciphertext()
			envelope.Clear()
			chunk.Storage = backupconfiguration.BackupConfigChunkStorageControllerProtected
			chunk.Protected = &backupconfiguration.BackupConfigProtectedChunkPayload{
				EnvelopeVersion: uint8(metadata.Version),
				Cipher:          string(metadata.Cipher),
				DigestAlgorithm: string(
					metadata.Digest.Algorithm,
				),
				CiphertextSHA256: metadata.Digest.Value,
				Ciphertext:       ciphertext,
			}
		} else {
			payload.Content = bytes.Clone(content)
			chunk.Storage, chunk.Plain = backupconfiguration.BackupConfigChunkStoragePlain, &payload
		}
		chunks = append(chunks, chunk)
		if end == len(value) {
			break
		}
		offset = end
	}
	return chunks, nil
}

func clearConfigSnapshotChunks(chunks []backupconfiguration.BackupConfigSnapshotValueChunkRecord) {
	for _, chunk := range chunks {
		if chunk.Plain != nil {
			clear(chunk.Plain.Content)
		}
		if chunk.Protected != nil {
			clear(chunk.Protected.Ciphertext)
		}
	}
}
