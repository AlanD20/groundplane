package backupconfigtransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// StreamTransport persists a validated receiver credit before its grant may
// authorize further frames. A successful stream write alone is not acceptance.
type StreamTransport interface {
	SendFrame(context.Context, *agentpb.BackupConfigTransfer) error
	ReadCredit(context.Context) (*agentpb.BackupConfigCredit, error)
	CommitCredit(context.Context, *agentpb.BackupConfigCredit) error
}

// ReceiverTransport delivers only credits already committed by the receiver.
// It takes ownership of each incoming frame until ReadFrame returns it.
type ReceiverTransport interface {
	ReadFrame(context.Context) (*agentpb.BackupConfigTransfer, error)
	SendCredit(context.Context, *agentpb.BackupConfigCredit) error
}

type OpenStreamValue func(context.Context, uint32) (io.ReadCloser, error)
type CommitStreamValue func(context.Context, uint32) error

type StreamInput struct {
	Binding   Binding
	Content   *agentpb.BackupConfigContentAuthority
	Headers   []*agentpb.BackupConfigEntryHeader
	OpenValue OpenStreamValue
	// CommitValue, when present, advances an owned restore pass only after the
	// receiver's durable credit proves this EntryEnd. Capture has no such cursor.
	CommitValue      CommitStreamValue
	Transport        StreamTransport
	PersistedCredits []*agentpb.BackupConfigCredit
	ReceiverCredit   *agentpb.BackupConfigCredit
}

// StreamResult is evidence of the final receiver-committed cursor, not live
// Entry publication. Both digests were reconstructed from actual selected bytes.
type StreamResult struct {
	TranscriptSHA256         [sha256.Size]byte
	MetadataTranscriptSHA256 [sha256.Size]byte
	Committed                Cursor
}

type streamSender struct {
	binding       Binding
	window        *SenderWindow
	transport     StreamTransport
	transcript    *backupconfig.TransferTranscriptWriter
	replayCredits []*agentpb.BackupConfigCredit
	replayIndex   int
	nativeCredits int
	replayThrough uint64
}

// Stream reconstructs the exact canonical records and credit chain against
// immutable selected bytes. Capture and Restore use this single delivery path;
// reconnect suppresses already committed records, not their source validation.
func Stream(ctx context.Context, input StreamInput) (StreamResult, error) {
	if ctx == nil || input.Transport == nil || input.OpenValue == nil || input.Binding.Validate() != nil {
		return StreamResult{}, invalidStream()
	}
	binding := input.Binding
	source, err := prepareStreamSource(ctx, binding.Direction, input.Content, input.Headers)
	if err != nil {
		return StreamResult{}, err
	}
	defer source.clear()
	replay, err := streamReplayCredits(binding, input.PersistedCredits, input.ReceiverCredit)
	if err != nil {
		return StreamResult{}, err
	}
	window, err := NewSenderWindow(binding, replay[0])
	if err != nil {
		return StreamResult{}, err
	}
	direction := backupconfig.TransferCapture
	if binding.Direction == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
		direction = backupconfig.TransferRestore
	}
	values := make([]backupconfig.ValueFrame, len(source.entries))
	for index, entry := range source.entries {
		values[index] = backupconfig.ValueFrame{
			Ordinal:   uint32(index + 1),
			EntryID:   entry.ID,
			SizeBytes: entry.Value.SizeBytes,
		}
	}
	transcript, err := backupconfig.NewTransferTranscriptWriter(
		ctx,
		io.Discard,
		direction,
		source.authority,
		source.metadata,
		values,
	)
	if err != nil {
		return StreamResult{}, err
	}
	sender := &streamSender{binding: binding, window: window, transport: input.Transport, transcript: transcript,
		replayCredits: replay, replayIndex: 1, nativeCredits: len(input.PersistedCredits),
		replayThrough: replay[len(replay)-1].CommittedRecordSequence}
	start := sender.frame()
	start.Record = &agentpb.BackupConfigTransfer_Start{Start: &agentpb.BackupConfigTransferStart{
		Direction: binding.Direction, Content: proto.CloneOf(source.content)}}
	if err := sender.sendMetadata(ctx, start); err != nil {
		return StreamResult{}, err
	}
	for _, metadata := range source.headers {
		header := sender.frame()
		header.Record = &agentpb.BackupConfigTransfer_EntryHeader{EntryHeader: proto.CloneOf(metadata)}
		if err := sender.sendMetadata(ctx, header); err != nil {
			return StreamResult{}, err
		}
	}
	for !window.Status().MetadataAccepted {
		if err := sender.acceptCredit(ctx); err != nil {
			return StreamResult{}, err
		}
	}
	for ordinal := uint32(1); ordinal <= uint32(len(source.entries)); ordinal++ {
		if err := sender.sendValue(ctx, source.entries[ordinal-1], ordinal, input.OpenValue, input.CommitValue); err != nil {
			return StreamResult{}, err
		}
	}
	digest, err := transcript.Finish(ctx)
	if err != nil {
		return StreamResult{}, err
	}
	end := sender.frame()
	end.Record = &agentpb.BackupConfigTransfer_End{End: &agentpb.BackupConfigTransferEnd{
		Direction: binding.Direction, Content: proto.CloneOf(source.content), TranscriptSha256: digest[:]}}
	if err := sender.sendValueFrame(ctx, end, 0); err != nil {
		return StreamResult{}, err
	}
	// Sending End is not receiver acceptance. Wait for a durable credit that
	// commits this exact final sequence/chain before reporting transfer success.
	for window.Status().Committed.RecordSequence != end.RecordSequence {
		if err := sender.acceptCredit(ctx); err != nil {
			return StreamResult{}, err
		}
	}
	return StreamResult{
		TranscriptSHA256:         digest,
		MetadataTranscriptSHA256: transcript.MetadataSHA256(),
		Committed:                window.Status().Committed,
	}, nil
}

func (sender *streamSender) frame() *agentpb.BackupConfigTransfer {
	return &agentpb.BackupConfigTransfer{TaskId: sender.binding.TaskID, AssignmentId: sender.binding.AssignmentID,
		StepId: sender.binding.StepID, ExecutionId: sender.binding.ExecutionID, TransferId: sender.binding.TransferID,
		RecordSequence: sender.window.Status().LastSentRecordSequence + 1}
}

func (sender *streamSender) sendMetadata(ctx context.Context, frame *agentpb.BackupConfigTransfer) error {
	for sender.window.Status().MetadataRecordCredit == 0 || sender.window.Status().MetadataByteCredit < uint64(proto.Size(frame)) {
		if err := sender.acceptCredit(ctx); err != nil {
			return err
		}
	}
	return sender.sendFrame(ctx, frame)
}

func (sender *streamSender) sendValueFrame(
	ctx context.Context,
	frame *agentpb.BackupConfigTransfer,
	bytes uint64,
) error {
	for sender.window.Status().ValueRecordCredit == 0 || sender.window.Status().ValueByteCredit < bytes {
		if err := sender.acceptCredit(ctx); err != nil {
			return err
		}
	}
	return sender.sendFrame(ctx, frame)
}

func (sender *streamSender) acceptCredit(ctx context.Context) error {
	if sender.replayIndex != len(sender.replayCredits) {
		return invalidStream()
	}
	credit, err := sender.transport.ReadCredit(ctx)
	if err != nil {
		return err
	}
	return sender.applyCredit(ctx, credit, true)
}

func (sender *streamSender) applyCredit(
	ctx context.Context,
	credit *agentpb.BackupConfigCredit,
	commit bool,
) error {
	if accepted := credit.GetMetadataAccepted(); accepted != nil {
		metadata := sender.transcript.MetadataSHA256()
		if !bytes.Equal(accepted.MetadataTranscriptSha256, metadata[:]) {
			return invalidStream()
		}
	}
	if err := sender.window.ApplyCredit(credit); err != nil {
		return err
	}
	if commit {
		return sender.transport.CommitCredit(ctx, credit)
	}
	return nil
}

func invalidStream() error {
	return invalid("Config stream differs from its sealed source or durable credit cursor")
}
