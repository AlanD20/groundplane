package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// CapturedConfigSnapshot authenticates durable metadata before transfer and
// decrypts only one selected Entry at a time. A returned value stream owns its
// plaintext: consume or close it before selecting another Entry.
type CapturedConfigSnapshot struct {
	snapshot  *backupconfiguration.CapturedSnapshotReader
	protector *secretvalue.Protector
	authority *agentpb.BackupConfigCaptureAuthority
	metadata  []*agentpb.BackupConfigEntry
	entries   []backupconfig.Entry
}

func (producer *ConfigSnapshotProducer) OpenCapturedConfigSnapshot(
	ctx context.Context,
	input backupplanning.BackupConfigSnapshotInput,
	expected *agentpb.BackupConfigCaptureAuthority,
) (*CapturedConfigSnapshot, error) {
	if ctx == nil || producer == nil || expected == nil || expected.Content == nil {
		return nil, configSnapshotInvalid()
	}
	snapshot, err := producer.snapshots.OpenCapturedSnapshot(ctx, backupconfiguration.CaptureSnapshotWriteAuthority{
		TaskID: input.TaskID, SnapshotID: input.SnapshotID, EnvironmentID: input.EnvironmentID, SourceID: input.SourceID,
		ReadRevision: input.ReadRevision, Expected: expected})
	if err != nil {
		return nil, err
	}
	reader := &CapturedConfigSnapshot{
		snapshot:  snapshot,
		protector: producer.protector,
		authority: proto.CloneOf(expected),
		metadata: make(
			[]*agentpb.BackupConfigEntry,
			snapshot.EntryCount(),
		),
		entries: make([]backupconfig.Entry, snapshot.EntryCount()),
	}
	for ordinal := uint64(0); ordinal < snapshot.EntryCount(); ordinal++ {
		stored, err := snapshot.ReadEntry(ctx, ordinal)
		if err != nil {
			return nil, err
		}
		metadata, entry, err := decodeCapturedConfigMetadata(stored, ordinal)
		stored.Clear()
		if err != nil {
			return nil, err
		}
		reader.metadata[ordinal], reader.entries[ordinal] = metadata, entry
	}
	encoder := func(ctx context.Context, direction backupconfig.TransferDirection, ordinal uint32, entry backupconfig.Entry) ([]byte, error) {
		if direction != backupconfig.TransferCapture || ordinal == 0 || int(ordinal) > len(reader.metadata) ||
			reader.metadata[ordinal-1].EntryId != entry.ID {
			return nil, configSnapshotInvalid()
		}
		return proto.MarshalOptions{Deterministic: true}.Marshal(reader.metadata[ordinal-1])
	}
	manifest, content, frames, err := backupconfig.BuildManifest(
		ctx,
		backupconfig.TransferCapture,
		reader.entries,
		encoder,
	)
	if err != nil {
		return nil, err
	}
	defer clear(manifest)
	defer func() {
		for _, frame := range frames {
			clear(frame.CanonicalEntry)
		}
	}()
	metadataSHA, metadataBytes, err := backupconfig.MetadataSnapshotSHA256(ctx, reader.entries, frames)
	if err != nil {
		return nil, err
	}
	actual := &agentpb.BackupConfigCaptureAuthority{EnvironmentId: input.EnvironmentID,
		MetadataSnapshotRevision: input.ReadRevision, MetadataEntryCount: content.EntryCount, MetadataProtoBytes: metadataBytes,
		Content: &agentpb.BackupConfigContentAuthority{
			ManifestSha256:          content.ManifestSHA256[:],
			EntryCount:              content.EntryCount,
			TotalSelectedValueBytes: content.TotalSelectedValueBytes,
			ManifestSizeBytes:       content.ManifestSizeBytes,
			SourceSizeBytes:         content.SourceSizeBytes,
			MetadataSnapshotSha256:  metadataSHA[:],
		}}
	if !configSnapshotSameAuthority(actual, expected) {
		return nil, configSnapshotInvalid()
	}
	return reader, nil
}

func (reader *CapturedConfigSnapshot) EntryCount() uint32 { return uint32(len(reader.metadata)) }

// ReadMetadata does not decrypt selected values. Metadata must be completely
// accepted before the producer opens the first value stream.
func (reader *CapturedConfigSnapshot) ReadMetadata(
	ctx context.Context,
	ordinal uint32,
) (*agentpb.BackupConfigEntry, error) {
	if reader == nil || ctx == nil || ordinal == 0 || int(ordinal) > len(reader.metadata) {
		return nil, configSnapshotInvalid()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return proto.CloneOf(reader.metadata[ordinal-1]), nil
}

func (reader *CapturedConfigSnapshot) Evidence() backupconfiguration.CaptureSnapshotEvidence {
	return reader.snapshot.Evidence()
}

// ReadEntry authenticates the entire selected value before exposing any of
// its plaintext. No success or artifact publication follows from a partial
// read; the typed transfer still owns its complete transcript/end validation.
func (reader *CapturedConfigSnapshot) ReadEntry(
	ctx context.Context,
	ordinal uint32,
) (*agentpb.BackupConfigEntry, io.ReadCloser, error) {
	if reader == nil || ctx == nil || ordinal == 0 || int(ordinal) > len(reader.metadata) {
		return nil, nil, configSnapshotInvalid()
	}
	stored, err := reader.snapshot.ReadEntry(ctx, uint64(ordinal-1))
	if err != nil {
		return nil, nil, err
	}
	defer stored.Clear()
	metadata, entry, err := decodeCapturedConfigMetadata(stored, uint64(ordinal-1))
	if err != nil || !proto.Equal(metadata, reader.metadata[ordinal-1]) {
		return nil, nil, configSnapshotInvalid()
	}
	value, err := reader.readSelectedValue(ctx, stored, entry)
	if err != nil {
		return nil, nil, err
	}
	if err := ctx.Err(); err != nil {
		clear(value)
		return nil, nil, err
	}
	return proto.CloneOf(metadata), entrymaterialization.OwnBytes(value), nil
}

func (reader *CapturedConfigSnapshot) readSelectedValue(
	ctx context.Context,
	stored *backupconfiguration.CapturedSnapshotEntry,
	entry backupconfig.Entry,
) (_ []byte, resultErr error) {
	if entry.Value.SizeBytes > backupconfig.MaxSelectedValueBytes {
		return nil, configSnapshotInvalid()
	}
	expectedChunks := (entry.Value.SizeBytes + backupconfiguration.MaximumBackupConfigChunkPayloadBytes - 1) / backupconfiguration.MaximumBackupConfigChunkPayloadBytes
	if entry.Secret && expectedChunks == 0 {
		expectedChunks = 1
	}
	if uint64(len(stored.Values)) != expectedChunks {
		return nil, configSnapshotInvalid()
	}
	value := make([]byte, 0, int(entry.Value.SizeBytes))
	defer func() {
		if resultErr != nil {
			clear(value)
		}
	}()
	for index := range stored.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		chunk := &stored.Values[index]
		consume := func(plaintext []byte) error {
			remaining := entry.Value.SizeBytes - uint64(len(value))
			length := uint64(backupconfiguration.MaximumBackupConfigChunkPayloadBytes)
			if remaining < length {
				length = remaining
			}
			if uint64(len(plaintext)) != length {
				return configSnapshotInvalid()
			}
			value = append(value, plaintext...)
			return nil
		}
		if !entry.Secret {
			if chunk.Plain == nil || chunk.Protected != nil || chunk.Plain.Offset != uint64(len(value)) {
				return nil, configSnapshotInvalid()
			}
			if err := consume(chunk.Plain.Content); err != nil {
				return nil, err
			}
			continue
		}
		if chunk.Protected == nil || chunk.Plain != nil {
			return nil, configSnapshotInvalid()
		}
		protected := chunk.Protected
		envelope, err := secretvalue.RestoreOwned(
			secretvalue.Metadata{Version: secretvalue.EnvelopeVersion(protected.EnvelopeVersion),
				Cipher: secretvalue.CipherSuite(
					protected.Cipher,
				), Digest: secretvalue.Digest{Algorithm: secretvalue.DigestAlgorithm(protected.DigestAlgorithm), Value: protected.CiphertextSHA256}},
			protected.Ciphertext,
		)
		protected.Ciphertext = nil
		if err != nil {
			return nil, err
		}
		err = reader.protector.OpenOwned(ctx, &envelope, consume)
		envelope.Clear()
		if err != nil {
			return nil, err
		}
	}
	if uint64(len(value)) != entry.Value.SizeBytes || sha256.Sum256(value) != entry.Value.SHA256 ||
		((entry.Source.Kind == backupconfig.SourceLiteral || entry.Metadata.Kind == backupconfig.MetadataEnvironment) && !utf8.Valid(value)) ||
		(entry.Metadata.Kind == backupconfig.MetadataEnvironment && bytes.IndexByte(value, 0) >= 0) {
		return nil, configSnapshotInvalid()
	}
	if !entry.Secret &&
		(stored.Record.PlainValueLength != uint64(len(value)) || stored.Record.PlainValueSHA256 != hex.EncodeToString(entry.Value.SHA256[:])) {
		return nil, configSnapshotInvalid()
	}
	return value, nil
}

func decodeCapturedConfigMetadata(
	stored *backupconfiguration.CapturedSnapshotEntry,
	ordinal uint64,
) (*agentpb.BackupConfigEntry, backupconfig.Entry, error) {
	metadata := &agentpb.BackupConfigEntry{}
	if stored == nil || proto.Unmarshal(stored.Descriptor, metadata) != nil ||
		executionplan.RejectUnknown(metadata) != nil ||
		metadata.Ordinal != uint32(ordinal+1) ||
		metadata.EntryId != stored.Record.EntryID ||
		metadata.Secret == nil ||
		*metadata.Secret != stored.Record.Secret ||
		metadata.Entry == nil ||
		metadata.Entry.ModRevision != stored.Record.EntryRevision ||
		len(metadata.Entry.Sha256) != sha256.Size ||
		metadata.SelectedValueSizeBytes > backupconfig.MaxSelectedValueBytes ||
		len(metadata.SelectedValueSha256) != sha256.Size {
		return nil, backupconfig.Entry{}, configSnapshotInvalid()
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(metadata)
	if err != nil {
		return nil, backupconfig.Entry{}, configSnapshotInvalid()
	}
	defer clear(canonical)
	if !bytes.Equal(canonical, stored.Descriptor) {
		return nil, backupconfig.Entry{}, configSnapshotInvalid()
	}
	entry, err := backupconfigtransfer.CaptureEntry(metadata)
	if err != nil {
		return nil, backupconfig.Entry{}, err
	}
	return metadata, entry, nil
}
