package backup

import (
	"context"
	"crypto/sha256"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// StreamCapture supplies immutable snapshot metadata and one selected value
// reader to the shared Config sender. It does not own another transfer loop.
func (reader *CapturedConfigSnapshot) StreamCapture(ctx context.Context,
	binding backupconfigtransfer.Binding, transport backupconfigtransfer.StreamTransport,
	persistedCredits []*agentpb.BackupConfigCredit, receiverCredit *agentpb.BackupConfigCredit,
) ([sha256.Size]byte, error) {
	if reader == nil || reader.authority == nil ||
		binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE {
		return [sha256.Size]byte{}, configSnapshotInvalid()
	}
	headers := make([]*agentpb.BackupConfigEntryHeader, len(reader.metadata))
	for index, metadata := range reader.metadata {
		size := reader.entries[index].Value.SizeBytes
		headers[index] = &agentpb.BackupConfigEntryHeader{
			Entry:      &agentpb.BackupConfigEntryHeader_CaptureEntry{CaptureEntry: proto.CloneOf(metadata)},
			ChunkCount: uint32((size + backupconfig.TransferChunkBytes - 1) / backupconfig.TransferChunkBytes),
		}
	}
	result, err := backupconfigtransfer.Stream(ctx, backupconfigtransfer.StreamInput{
		Binding: binding, Content: reader.authority.Content, Headers: headers, Transport: transport,
		PersistedCredits: persistedCredits, ReceiverCredit: receiverCredit,
		OpenValue: func(ctx context.Context, ordinal uint32) (io.ReadCloser, error) {
			_, source, err := reader.ReadEntry(ctx, ordinal)
			return source, err
		},
	})
	return result.TranscriptSHA256, err
}
