package backup

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type configRestoreTransferStore interface {
	ReadConfigTransferCursor(
		context.Context,
		string,
		string,
		string,
		string,
		int64,
	) (etcdstore.Versioned[backupconfiguration.ConfigTransferCursor], bool, error)
	VisitConfigTransferCredits(
		context.Context,
		etcdstore.Versioned[backupconfiguration.ConfigTransferCursor],
		func(*agentpb.BackupConfigCredit) error,
	) error
	VisitConfigRestoreTransferRecords(
		context.Context,
		backupconfiguration.ConfigRestoreTransferOwner,
		etcdstore.Versioned[backupconfiguration.ConfigTransferCursor],
		uint64,
		uint64,
		func(backupconfiguration.ConfigRestoreTransferRecord) error,
	) error
	CommitConfigRestoreTransfer(
		context.Context,
		backupconfiguration.ConfigRestoreTransferBatch,
		*agentpb.BackupConfigCredit,
	) (int64, error)
}

// ConfigRestoreReceiver serializes one transfer. Live Entry mutation is not
// part of this owner: it stores protected recovery input and durable credits.
// The channel must deliver returned credits only after these methods succeed.
type ConfigRestoreReceiver struct {
	owner     backupconfiguration.ConfigRestoreTransferOwner
	store     configRestoreTransferStore
	protector *secretvalue.Protector
	receiver  *backupconfigtransfer.Receiver
	pending   []backupconfiguration.ConfigRestoreTransferRecord
	credit    *agentpb.BackupConfigCredit
	failed    bool
	expected  *agentpb.BackupConfigContentAuthority
}

// OpenConfigRestoreReceiver reconstructs only committed frames from a fixed
// revision. Unacknowledged input is retransmitted by the Agent, not adopted
// from memory. Archive reconstruction discards output; values remain protected
// in the journal until a separate generation publisher selects them.
func OpenConfigRestoreReceiver(
	ctx context.Context,
	owner backupconfiguration.ConfigRestoreTransferOwner,
	expected *agentpb.BackupConfigContentAuthority,
	store configRestoreTransferStore,
	protector *secretvalue.Protector,
) (*ConfigRestoreReceiver, *agentpb.BackupConfigCredit, error) {
	if ctx == nil || backupconfiguration.ValidateConfigRestoreTransferOwner(owner) != nil || store == nil ||
		protector == nil {
		return nil, nil, configSnapshotInvalid()
	}
	contentSHA, err := backupconfigtransfer.ContentSHA256(expected)
	if err != nil || hex.EncodeToString(contentSHA) != owner.ContentSHA256 ||
		expected.SourceSizeBytes != owner.SourceSizeBytes {
		return nil, nil, configSnapshotGuardConflict()
	}
	transfer := &ConfigRestoreReceiver{owner: owner, store: store, protector: protector,
		expected: proto.CloneOf(expected),
		pending: make(
			[]backupconfiguration.ConfigRestoreTransferRecord,
			0,
			backupconfigtransfer.MetadataCreditRecords,
		)}
	binding := owner.Transfer.Binding
	cursor, found, err := store.ReadConfigTransferCursor(
		ctx,
		binding.TaskID,
		binding.AssignmentID,
		binding.StepID,
		binding.ExecutionID,
		0,
	)
	if err != nil {
		return nil, nil, err
	}
	if found {
		credit, err := transfer.recover(ctx, expected, cursor)
		if err != nil {
			transfer.Abort()
			return nil, nil, err
		}
		return transfer, credit, nil
	}
	credit, err := backupconfigtransfer.InitialCredit(binding)
	if err != nil {
		return nil, nil, err
	}
	transfer.receiver, err = backupconfigtransfer.NewReceiver(binding, expected, io.Discard, credit)
	if err != nil {
		return nil, nil, err
	}
	if _, err := store.CommitConfigRestoreTransfer(ctx, backupconfiguration.ConfigRestoreTransferBatch{Owner: owner}, credit); err != nil {
		transfer.Abort()
		return nil, nil, err
	}
	return transfer, credit, nil
}

// AcceptFrame protects actual validated input before acknowledging it. On an
// ambiguous durable write only Flush may be retried with the same prepared
// ciphertext. A protection/validation failure aborts this receiver entirely.
func (transfer *ConfigRestoreReceiver) AcceptFrame(
	ctx context.Context,
	frame *agentpb.BackupConfigTransfer,
) (*agentpb.BackupConfigCredit, error) {
	if transfer == nil || transfer.failed || transfer.credit != nil ||
		len(transfer.pending) >= int(backupconfigtransfer.MetadataCreditRecords) {
		return nil, configSnapshotGuardConflict()
	}
	if err := transfer.receiver.AcceptFrame(ctx, frame); err != nil {
		transfer.Abort()
		return nil, err
	}
	if err := transfer.verifyCompleteSource(); err != nil {
		transfer.Abort()
		return nil, err
	}
	record, err := transfer.protect(ctx, frame)
	if err != nil {
		transfer.Abort()
		return nil, err
	}
	transfer.pending = append(transfer.pending, record)
	transfer.credit, err = transfer.receiver.NextCredit()
	if err != nil {
		transfer.Abort()
		return nil, err
	}
	return transfer.Flush(ctx)
}

func (transfer *ConfigRestoreReceiver) Flush(ctx context.Context) (*agentpb.BackupConfigCredit, error) {
	if transfer == nil || transfer.failed || ctx == nil {
		return nil, configSnapshotInvalid()
	}
	if transfer.credit == nil {
		return nil, nil
	}
	credit := transfer.credit
	_, err := transfer.store.CommitConfigRestoreTransfer(ctx, backupconfiguration.ConfigRestoreTransferBatch{
		Owner: transfer.owner, Records: transfer.pending,
	}, credit)
	if err != nil {
		return nil, err
	}
	if err := transfer.receiver.AcceptPersistedCredit(credit); err != nil {
		transfer.Abort()
		return nil, err
	}
	backupconfiguration.ClearConfigRestoreTransferRecords(transfer.pending)
	transfer.pending = transfer.pending[:0]
	transfer.credit = nil
	return proto.CloneOf(credit), nil
}

func (transfer *ConfigRestoreReceiver) Progress() backupconfigtransfer.ReceiverProgress {
	if transfer == nil || transfer.failed || len(transfer.pending) != 0 || transfer.credit != nil {
		return backupconfigtransfer.ReceiverProgress{}
	}
	return transfer.receiver.Progress()
}

// Completed proves durable receipt of the complete selected source, not live
// publication or file materialization.
func (transfer *ConfigRestoreReceiver) Completed() (*agentpb.BackupConfigTransferCompleted, error) {
	progress := transfer.Progress()
	if !progress.Complete || transfer.verifyCompleteSource() != nil {
		return nil, configSnapshotGuardConflict()
	}
	return &agentpb.BackupConfigTransferCompleted{
		RestoreGenerationId: transfer.owner.GenerationID, RenderGeneration: transfer.owner.RenderGeneration,
		Content: proto.CloneOf(transfer.expected), CommittedRecordCount: progress.Cursor.RecordSequence,
		ValueChainSha256:         bytes.Clone(progress.Cursor.ChainSHA256[:]),
		TransferTranscriptSha256: bytes.Clone(progress.TranscriptSHA256[:]),
	}, nil
}

func (transfer *ConfigRestoreReceiver) verifyCompleteSource() error {
	progress := transfer.receiver.Progress()
	if progress.Complete && (progress.Source.SizeBytes != transfer.owner.SourceSizeBytes ||
		hex.EncodeToString(progress.Source.SHA256[:]) != transfer.owner.SourceSHA256) {
		return configSnapshotGuardConflict()
	}
	return nil
}

func (transfer *ConfigRestoreReceiver) Abort() {
	if transfer != nil {
		transfer.failed = true
		backupconfiguration.ClearConfigRestoreTransferRecords(transfer.pending)
		transfer.pending, transfer.credit = nil, nil
		if transfer.receiver != nil {
			transfer.receiver.Abort()
		}
	}
}

func (transfer *ConfigRestoreReceiver) protect(
	ctx context.Context,
	frame *agentpb.BackupConfigTransfer,
) (backupconfiguration.ConfigRestoreTransferRecord, error) {
	var zero backupconfiguration.ConfigRestoreTransferRecord
	owned, err := backupconfigtransfer.ValidateFrame(transfer.owner.Transfer.Binding, frame)
	if err != nil {
		return zero, err
	}
	defer backupconfigtransfer.ClearFrame(owned)
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(owned)
	if err != nil {
		return zero, configSnapshotInvalid()
	}
	defer clear(raw)
	envelope, err := transfer.protector.Seal(ctx, raw)
	if err != nil {
		return zero, err
	}
	defer envelope.Clear()
	metadata := envelope.Metadata()
	record := backupconfiguration.ConfigRestoreTransferRecord{Owner: transfer.owner, Sequence: owned.RecordSequence,
		Protected: backupconfiguration.BackupConfigProtectedChunkPayload{
			EnvelopeVersion: uint8(metadata.Version), Cipher: string(metadata.Cipher),
			DigestAlgorithm: string(metadata.Digest.Algorithm), CiphertextSHA256: metadata.Digest.Value,
			Ciphertext: envelope.Ciphertext(),
		}}
	if _, err := backupconfiguration.EncodeConfigRestoreTransferRecord(record); err != nil {
		clear(record.Protected.Ciphertext)
		return zero, err
	}
	return record, nil
}

func (transfer *ConfigRestoreReceiver) openRecord(
	ctx context.Context,
	record backupconfiguration.ConfigRestoreTransferRecord,
) error {
	return openConfigRestoreRecord(
		ctx,
		transfer.owner,
		transfer.protector,
		record,
		func(frame *agentpb.BackupConfigTransfer) error {
			return transfer.receiver.AcceptFrame(ctx, frame)
		},
	)
}

func openConfigRestoreRecord(ctx context.Context, owner backupconfiguration.ConfigRestoreTransferOwner,
	protector *secretvalue.Protector, record backupconfiguration.ConfigRestoreTransferRecord,
	visit func(*agentpb.BackupConfigTransfer) error,
) error {
	if ctx == nil || protector == nil || visit == nil || record.Owner != owner {
		return configSnapshotGuardConflict()
	}
	payload := record.Protected
	envelope, err := secretvalue.Restore(secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion(payload.EnvelopeVersion), Cipher: secretvalue.CipherSuite(payload.Cipher),
		Digest: secretvalue.Digest{
			Algorithm: secretvalue.DigestAlgorithm(payload.DigestAlgorithm),
			Value:     payload.CiphertextSHA256,
		},
	}, payload.Ciphertext)
	if err != nil {
		return err
	}
	defer envelope.Clear()
	return protector.Open(ctx, envelope, func(raw []byte) error {
		frame := &agentpb.BackupConfigTransfer{}
		if proto.Unmarshal(raw, frame) != nil {
			backupconfigtransfer.ClearFrame(frame)
			return configSnapshotInvalid()
		}
		defer backupconfigtransfer.ClearFrame(frame)
		canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(frame)
		defer clear(canonical)
		if err != nil || !bytes.Equal(raw, canonical) || frame.RecordSequence != record.Sequence {
			return configSnapshotGuardConflict()
		}
		owned, err := backupconfigtransfer.ValidateFrame(owner.Transfer.Binding, frame)
		if err != nil {
			return err
		}
		defer backupconfigtransfer.ClearFrame(owned)
		return visit(owned)
	})
}

func (transfer *ConfigRestoreReceiver) recover(
	ctx context.Context,
	expected *agentpb.BackupConfigContentAuthority,
	cursor etcdstore.Versioned[backupconfiguration.ConfigTransferCursor],
) (*agentpb.BackupConfigCredit, error) {
	if cursor.Record.Owner != transfer.owner.Transfer {
		return nil, configSnapshotGuardConflict()
	}
	var previous *agentpb.BackupConfigCredit
	err := transfer.store.VisitConfigTransferCredits(ctx, cursor, func(credit *agentpb.BackupConfigCredit) error {
		if previous == nil {
			var err error
			transfer.receiver, err = backupconfigtransfer.NewReceiver(
				transfer.owner.Transfer.Binding,
				expected,
				io.Discard,
				credit,
			)
			previous = proto.CloneOf(credit)
			return err
		}
		err := transfer.store.VisitConfigRestoreTransferRecords(ctx, transfer.owner, cursor,
			previous.CommittedRecordSequence+1, credit.CommittedRecordSequence,
			func(record backupconfiguration.ConfigRestoreTransferRecord) error {
				return transfer.openRecord(ctx, record)
			})
		if err != nil {
			return err
		}
		predicted, err := transfer.receiver.NextCredit()
		if err != nil || !proto.Equal(predicted, credit) {
			return configSnapshotGuardConflict()
		}
		if err := transfer.receiver.AcceptPersistedCredit(credit); err != nil {
			return err
		}
		previous = proto.CloneOf(credit)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if previous == nil {
		return nil, configSnapshotGuardConflict()
	}
	return previous, transfer.verifyCompleteSource()
}
