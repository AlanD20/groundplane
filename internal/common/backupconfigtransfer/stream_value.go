package backupconfigtransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (sender *streamSender) sendValue(
	ctx context.Context,
	entry backupconfig.Entry,
	ordinal uint32,
	openValue OpenStreamValue,
	commitValue CommitStreamValue,
) (resultErr error) {
	source, err := openValue(ctx, ordinal)
	if err != nil {
		return err
	}
	if source == nil {
		return invalidStream()
	}
	closed := false
	defer func() {
		if !closed {
			if closeErr := source.Close(); closeErr != nil {
				resultErr = errs.WrapJoined(errs.KindInternal, resultErr, closeErr)
			}
		}
	}()
	buffer := make([]byte, backupconfig.TransferChunkBytes)
	defer clear(buffer)
	hasher := sha256.New()
	var offset uint64
	for offset < entry.Value.SizeBytes {
		if err := ctx.Err(); err != nil {
			return err
		}
		length := uint64(len(buffer))
		if length > entry.Value.SizeBytes-offset {
			length = entry.Value.SizeBytes - offset
		}
		chunk := buffer[:int(length)]
		if _, err := io.ReadFull(source, chunk); err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		_, _ = hasher.Write(chunk)
		if err := sender.transcript.WriteValueChunk(ctx, ordinal, offset, chunk); err != nil {
			return err
		}
		frame := sender.frame()
		frame.Record = &agentpb.BackupConfigTransfer_ValueChunk{ValueChunk: &agentpb.BackupConfigValueChunk{
			Ordinal: ordinal, Offset: offset, Content: chunk}}
		err := sender.sendValueFrame(ctx, frame, length)
		clear(chunk)
		frame.GetValueChunk().Content = nil
		if err != nil {
			return err
		}
		offset += length
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var extra [1]byte
	count, eofErr := source.Read(extra[:])
	clear(extra[:])
	if count != 0 || eofErr != io.EOF || !bytes.Equal(hasher.Sum(nil), entry.Value.SHA256[:]) {
		return invalidStream()
	}
	// Closing releases the authenticated reader into its awaiting-commit state.
	// The durable EntryEnd credit, not reader EOF, advances a restore pass.
	closed = true
	if err := source.Close(); err != nil {
		return err
	}
	if err := sender.transcript.EndValue(ctx, ordinal); err != nil {
		return err
	}
	end := sender.frame()
	end.Record = &agentpb.BackupConfigTransfer_EntryEnd{EntryEnd: &agentpb.BackupConfigEntryEnd{
		Ordinal: ordinal, ValueSizeBytes: entry.Value.SizeBytes, ValueSha256: entry.Value.SHA256[:]}}
	if err := sender.sendValueFrame(ctx, end, 0); err != nil {
		return err
	}
	if commitValue != nil {
		for sender.window.Status().Committed.RecordSequence < end.RecordSequence {
			if err := sender.acceptCredit(ctx); err != nil {
				return err
			}
		}
		return commitValue(ctx, ordinal)
	}
	return nil
}
