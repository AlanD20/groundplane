package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type configRestoreGenerationStore interface {
	configRestoreTransferStore
	ReadConfigRestoreGeneration(context.Context, backupconfiguration.ConfigRestoreTransferOwner, int64) (
		etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord], bool, error,
	)
}

// ConfigRestoreGeneration is a fully verified fixed-view publication source.
// Opening it validates every protected record and the entire reconstructed
// archive before any Entry callback can receive selected bytes. It is not live
// publication authority; that writer must compare this seal and its native Task.
type ConfigRestoreGeneration struct {
	owner     backupconfiguration.ConfigRestoreTransferOwner
	seal      etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord]
	cursor    etcdstore.Versioned[backupconfiguration.ConfigTransferCursor]
	store     configRestoreGenerationStore
	protector *secretvalue.Protector
}

func OpenConfigRestoreGeneration(
	ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner,
	expected *agentpb.BackupConfigContentAuthority,
	store configRestoreGenerationStore,
	protector *secretvalue.Protector,
) (*ConfigRestoreGeneration, error) {
	if ctx == nil || store == nil || protector == nil ||
		backupconfiguration.ValidateConfigRestoreTransferOwner(owner) != nil {
		return nil, configSnapshotInvalid()
	}
	digest, err := backupconfigtransfer.ContentSHA256(expected)
	if err != nil || hex.EncodeToString(digest) != owner.ContentSHA256 {
		return nil, configSnapshotGuardConflict()
	}
	binding := owner.Transfer.Binding
	cursor, found, err := store.ReadConfigTransferCursor(ctx, binding.TaskID, binding.AssignmentID,
		binding.StepID, binding.ExecutionID, 0)
	if err != nil {
		return nil, err
	}
	if !found || cursor.Record.Owner != owner.Transfer {
		return nil, configSnapshotGuardConflict()
	}
	seal, found, err := store.ReadConfigRestoreGeneration(ctx, owner, cursor.ReadRevision)
	if err != nil {
		return nil, err
	}
	if !found || !proto.Equal(seal.Record.Completed.Content, expected) {
		return nil, configSnapshotGuardConflict()
	}
	reader := &ConfigRestoreReceiver{
		owner:     owner,
		store:     store,
		protector: protector,
		expected:  proto.CloneOf(expected),
	}
	defer reader.Abort()
	if _, err := reader.recover(ctx, expected, cursor); err != nil {
		return nil, err
	}
	completed, err := reader.Completed()
	if err != nil || !proto.Equal(completed, seal.Record.Completed) {
		return nil, configSnapshotGuardConflict()
	}
	return &ConfigRestoreGeneration{owner: owner, seal: seal, cursor: cursor, store: store, protector: protector}, nil
}

func (generation *ConfigRestoreGeneration) Seal() etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord] {
	owned := generation.seal
	owned.Record.Completed = proto.CloneOf(owned.Record.Completed)
	return owned
}

// VisitEntries owns at most one selected value (256 KiB), plus the bounded
// descriptor catalog. Bytes are cleared after the callback, including on error;
// callers must persist a protected generation rather than retaining plaintext.
func (generation *ConfigRestoreGeneration) VisitEntries(ctx context.Context,
	visit func(uint32, backupconfig.Entry, []byte) error,
) error {
	if generation == nil || ctx == nil || visit == nil {
		return configSnapshotInvalid()
	}
	content := generation.seal.Record.Completed.Content
	entries := make([]backupconfig.Entry, 0, content.EntryCount)
	var value []byte
	var ordinal uint32
	var offset uint64
	ended := false
	defer func() { clear(value) }()
	err := generation.store.VisitConfigRestoreTransferRecords(ctx, generation.owner, generation.cursor,
		1, generation.seal.Record.Completed.CommittedRecordCount,
		func(record backupconfiguration.ConfigRestoreTransferRecord) error {
			return openConfigRestoreRecord(ctx, generation.owner, generation.protector, record,
				func(frame *agentpb.BackupConfigTransfer) error {
					switch payload := frame.Record.(type) {
					case *agentpb.BackupConfigTransfer_Start:
						if frame.RecordSequence != 1 || !proto.Equal(payload.Start.Content, content) {
							return configSnapshotGuardConflict()
						}
					case *agentpb.BackupConfigTransfer_EntryHeader:
						if uint32(len(entries)) >= content.EntryCount || ordinal != 0 {
							return configSnapshotGuardConflict()
						}
						entry, err := backupconfigtransfer.RestoreEntry(payload.EntryHeader.GetRestoreEntry())
						if err != nil {
							return err
						}
						entries = append(entries, entry)
					case *agentpb.BackupConfigTransfer_ValueChunk:
						chunk := payload.ValueChunk
						if uint32(len(entries)) != content.EntryCount || ordinal >= content.EntryCount ||
							chunk.Ordinal != ordinal+1 || chunk.Offset != offset {
							return configSnapshotGuardConflict()
						}
						entry := entries[ordinal]
						if value == nil {
							value = make([]byte, int(entry.Value.SizeBytes))
						}
						if offset > uint64(len(value)) || uint64(len(chunk.Content)) > uint64(len(value))-offset {
							return configSnapshotGuardConflict()
						}
						copy(value[int(offset):], chunk.Content)
						offset += uint64(len(chunk.Content))
					case *agentpb.BackupConfigTransfer_EntryEnd:
						end := payload.EntryEnd
						if uint32(len(entries)) != content.EntryCount || ordinal >= content.EntryCount || end.Ordinal != ordinal+1 {
							return configSnapshotGuardConflict()
						}
						entry := entries[ordinal]
						digest := sha256.Sum256(value)
						if offset != entry.Value.SizeBytes || end.ValueSizeBytes != offset ||
							digest != entry.Value.SHA256 || !bytes.Equal(end.ValueSha256, digest[:]) {
							return configSnapshotGuardConflict()
						}
						err := visit(ordinal+1, entry, value)
						clear(value)
						value, offset = nil, 0
						ordinal++
						return err
					case *agentpb.BackupConfigTransfer_End:
						if ordinal != content.EntryCount || frame.RecordSequence != generation.seal.Record.Completed.CommittedRecordCount ||
							!proto.Equal(payload.End.Content, content) ||
							!bytes.Equal(payload.End.TranscriptSha256, generation.seal.Record.Completed.TransferTranscriptSha256) {
							return configSnapshotGuardConflict()
						}
						ended = true
					default:
						return configSnapshotGuardConflict()
					}
					return nil
				})
		})
	if err != nil {
		return err
	}
	if !ended || ordinal != content.EntryCount {
		return configSnapshotGuardConflict()
	}
	return nil
}
