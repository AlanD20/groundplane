package backupconfigtransfer

import (
	"context"
	"crypto/sha256"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ReceiverProgress describes validated input, not durable storage. A channel
// owner must durably persist its stage and cursor before granting credit.
type ReceiverProgress struct {
	Cursor                   Cursor
	MetadataReady            bool
	MetadataTranscriptSHA256 [sha256.Size]byte
	Complete                 bool
	Source                   backupconfig.ArtifactEvidence
	TranscriptSHA256         [sha256.Size]byte
}

// Receiver verifies one Config transfer and writes its canonical archive to a
// caller-owned destination. It never grants credit, publishes an artifact, or
// authorizes discard/resume of an interrupted stage.
type Receiver struct {
	binding     Binding
	expected    *agentpb.BackupConfigContentAuthority
	destination io.Writer
	window      *SenderWindow
	headers     []*agentpb.BackupConfigEntryHeader
	entries     []backupconfig.Entry
	layout      backupconfig.Layout
	artifact    *backupconfig.ArtifactWriter
	retained    *prefixReplayWriter
	transcript  *backupconfig.TransferTranscriptWriter
	progress    ReceiverProgress
	started     bool
	valueOffset uint64
	failed      bool
}

func NewReceiver(
	binding Binding,
	expected *agentpb.BackupConfigContentAuthority,
	destination io.Writer,
	persistedInitialCredit *agentpb.BackupConfigCredit,
) (*Receiver, error) {
	if !validContent(expected) || rejectUnknown(expected) != nil || destination == nil ||
		persistedInitialCredit == nil || persistedInitialCredit.CreditSequence != 1 ||
		persistedInitialCredit.CommittedRecordSequence != 0 || persistedInitialCredit.GetMetadataCredit() == nil {
		return nil, invalid("new Config receiver requires sealed content and initial persisted metadata credit")
	}
	window, err := NewSenderWindow(binding, persistedInitialCredit)
	if err != nil {
		return nil, err
	}
	receiver := &Receiver{binding: binding, expected: proto.CloneOf(expected), destination: destination,
		window: window, headers: make([]*agentpb.BackupConfigEntryHeader, 0, expected.EntryCount),
		entries: make([]backupconfig.Entry, 0, expected.EntryCount)}
	receiver.progress.Cursor = window.Status().Committed
	return receiver, nil
}

// AcceptPersistedCredit installs only a credit whose committed cursor matches
// records already validated by this receiver. Persistence belongs to the caller.
func (receiver *Receiver) AcceptPersistedCredit(credit *agentpb.BackupConfigCredit) error {
	if receiver == nil || receiver.failed {
		return invalid("Config receiver is unavailable")
	}
	if accepted := credit.GetMetadataAccepted(); accepted != nil &&
		(!receiver.progress.MetadataReady || !sameDigest(accepted.MetadataTranscriptSha256,
			receiver.progress.MetadataTranscriptSHA256[:])) {
		return receiver.fail(invalid("Config metadata credit does not match the validated transcript"))
	}
	if err := receiver.window.ApplyCredit(credit); err != nil {
		return receiver.fail(err)
	}
	return nil
}

func (receiver *Receiver) AcceptFrame(ctx context.Context, frame *agentpb.BackupConfigTransfer) error {
	if receiver == nil || receiver.failed || receiver.progress.Complete {
		return invalid("Config receiver is unavailable or complete")
	}
	if ctx == nil {
		return receiver.fail(invalid("Config transfer context is required"))
	}
	if err := ctx.Err(); err != nil {
		return receiver.fail(err)
	}
	owned, err := ValidateFrame(receiver.binding, frame)
	if err != nil {
		return receiver.fail(err)
	}
	defer ClearFrame(owned)
	chain, err := receiver.window.Send(owned)
	if err != nil {
		return receiver.fail(err)
	}
	switch record := owned.Record.(type) {
	case *agentpb.BackupConfigTransfer_Start:
		err = receiver.acceptStart(ctx, record.Start)
	case *agentpb.BackupConfigTransfer_EntryHeader:
		err = receiver.acceptHeader(ctx, record.EntryHeader)
	case *agentpb.BackupConfigTransfer_ValueChunk:
		err = receiver.acceptValue(ctx, record.ValueChunk)
	case *agentpb.BackupConfigTransfer_EntryEnd:
		err = receiver.acceptEntryEnd(ctx, record.EntryEnd)
	case *agentpb.BackupConfigTransfer_End:
		err = receiver.acceptEnd(ctx, record.End)
	case *agentpb.BackupConfigTransfer_Resume:
		if !receiver.progress.MetadataReady ||
			record.Resume.NextOrdinal != receiver.progress.Cursor.NextOrdinal {
			err = invalid("Config resume does not match the reconstructed value cursor")
		}
	default:
		err = invalid("Config transfer record is unsupported")
	}
	if err != nil {
		return receiver.fail(err)
	}
	status := receiver.window.Status()
	receiver.progress.Cursor.RecordSequence = owned.RecordSequence
	receiver.progress.Cursor.NextOrdinal = status.SentNextOrdinal
	copy(receiver.progress.Cursor.ChainSHA256[:], chain)
	return nil
}

func (receiver *Receiver) Progress() ReceiverProgress {
	if receiver == nil || receiver.failed {
		return ReceiverProgress{}
	}
	return receiver.progress
}

func (receiver *Receiver) Abort() {
	if receiver != nil {
		receiver.failed = true
		receiver.artifact.Abort()
	}
}

func (receiver *Receiver) acceptStart(ctx context.Context, start *agentpb.BackupConfigTransferStart) error {
	if receiver.started || !proto.Equal(start.Content, receiver.expected) {
		return invalid("Config transfer start differs from the sealed content authority")
	}
	receiver.started = true
	if receiver.expected.EntryCount == 0 {
		return receiver.sealMetadata(ctx)
	}
	return nil
}

func (receiver *Receiver) acceptHeader(ctx context.Context, header *agentpb.BackupConfigEntryHeader) error {
	if !receiver.started || receiver.progress.MetadataReady ||
		uint32(len(receiver.entries)) >= receiver.expected.EntryCount {
		return invalid("Config Entry header is outside the metadata phase")
	}
	ordinal := uint32(len(receiver.entries) + 1)
	if capture := header.GetCaptureEntry(); capture != nil && capture.Ordinal != ordinal {
		return invalid("Config capture metadata ordinal is not contiguous")
	}
	entry, err := entryFromHeader(receiver.binding.Direction, header)
	if err != nil {
		return err
	}
	if len(receiver.entries) > 0 && receiver.entries[len(receiver.entries)-1].ID >= entry.ID {
		return invalid("Config metadata is not in unique stable-ID order")
	}
	receiver.entries = append(receiver.entries, entry)
	receiver.headers = append(receiver.headers, proto.CloneOf(header))
	if uint32(len(receiver.entries)) == receiver.expected.EntryCount {
		return receiver.sealMetadata(ctx)
	}
	return nil
}

func (receiver *Receiver) acceptValue(ctx context.Context, chunk *agentpb.BackupConfigValueChunk) error {
	if !receiver.progress.MetadataReady || chunk.Ordinal != receiver.progress.Cursor.NextOrdinal ||
		chunk.Offset != receiver.valueOffset {
		return invalid("Config value chunk does not match the current Entry")
	}
	if err := receiver.artifact.WriteValueChunk(ctx, chunk.Ordinal, chunk.Offset, chunk.Content); err != nil {
		return err
	}
	if err := receiver.transcript.WriteValueChunk(ctx, chunk.Ordinal, chunk.Offset, chunk.Content); err != nil {
		return err
	}
	receiver.valueOffset += uint64(len(chunk.Content))
	return nil
}

func (receiver *Receiver) acceptEntryEnd(ctx context.Context, end *agentpb.BackupConfigEntryEnd) error {
	if !receiver.progress.MetadataReady || end.Ordinal != receiver.progress.Cursor.NextOrdinal ||
		end.Ordinal == 0 || int(end.Ordinal) > len(receiver.entries) {
		return invalid("Config Entry end cursor is invalid")
	}
	entry := receiver.entries[end.Ordinal-1]
	if end.ValueSizeBytes != entry.Value.SizeBytes || !sameDigest(end.ValueSha256, entry.Value.SHA256[:]) {
		return invalid("Config Entry end differs from the metadata authority")
	}
	if err := receiver.artifact.EndValue(ctx, end.Ordinal); err != nil {
		return err
	}
	if err := receiver.transcript.EndValue(ctx, end.Ordinal); err != nil {
		return err
	}
	receiver.valueOffset = 0
	return nil
}

func (receiver *Receiver) acceptEnd(ctx context.Context, end *agentpb.BackupConfigTransferEnd) error {
	if !receiver.progress.MetadataReady || receiver.progress.Cursor.NextOrdinal != receiver.expected.EntryCount+1 ||
		!proto.Equal(end.Content, receiver.expected) {
		return invalid("Config transfer ended before all sealed Entries were received")
	}
	transcript, err := receiver.transcript.Finish(ctx)
	if err != nil {
		return err
	}
	if !sameDigest(end.TranscriptSha256, transcript[:]) {
		return invalid("Config transfer transcript digest does not match")
	}
	source, err := receiver.artifact.Finish(ctx)
	if err != nil {
		return err
	}
	if receiver.retained != nil && !receiver.retained.consumed() {
		return invalid("Config completion leaves an unverified retained tail")
	}
	receiver.progress.TranscriptSHA256, receiver.progress.Source, receiver.progress.Complete = transcript, source, true
	return nil
}

func (receiver *Receiver) fail(err error) error { receiver.Abort(); return err }
