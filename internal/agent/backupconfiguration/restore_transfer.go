package backupconfiguration

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// StreamRestore sends only fully authenticated pass-two values. Each Entry
// cursor advances after the Controller durably accepts its complete selected
// value; reconnect reconstructs the same records using the shared sender.
func (artifact *RestoreArtifact) StreamRestore(ctx context.Context,
	binding backupconfigtransfer.Binding, transport backupconfigtransfer.StreamTransport,
	persistedCredits []*agentpb.BackupConfigCredit, receiverCredit *agentpb.BackupConfigCredit,
) (*agentpb.BackupConfigTransferCompleted, error) {
	if artifact == nil || artifact.evidence == nil || artifact.authority == nil ||
		binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
		return nil, invalidRestoreArtifact()
	}
	pass, err := artifact.BeginTransfer(ctx)
	if err != nil {
		return nil, err
	}
	metadata := pass.MetadataFrames()
	defer func() {
		for _, frame := range metadata {
			clear(frame.CanonicalEntry)
		}
	}()
	headers := make([]*agentpb.BackupConfigEntryHeader, len(metadata))
	for index, frame := range metadata {
		value := &agentpb.BackupConfigRestoreEntry{}
		if err := proto.Unmarshal(frame.CanonicalEntry, value); err != nil {
			return nil, err
		}
		entry, err := backupconfigtransfer.RestoreEntry(value)
		if err != nil {
			return nil, err
		}
		if frame.Ordinal != uint32(index+1) || frame.EntryID != entry.ID {
			return nil, invalidRestoreArtifact()
		}
		headers[index] = &agentpb.BackupConfigEntryHeader{
			Entry: &agentpb.BackupConfigEntryHeader_RestoreEntry{RestoreEntry: value},
			ChunkCount: uint32(
				(entry.Value.SizeBytes + backupconfig.TransferChunkBytes - 1) / backupconfig.TransferChunkBytes,
			),
		}
	}
	result, err := backupconfigtransfer.Stream(ctx, backupconfigtransfer.StreamInput{
		Binding: binding, Content: artifact.evidence.GetConfig().Content, Headers: headers, Transport: transport,
		PersistedCredits: persistedCredits, ReceiverCredit: receiverCredit,
		OpenValue: func(ctx context.Context, ordinal uint32) (io.ReadCloser, error) {
			_, reader, err := pass.OpenValue(ctx, int(ordinal-1))
			return reader, err
		},
		CommitValue: func(ctx context.Context, ordinal uint32) error { return pass.CommitValue(ctx, int(ordinal-1)) },
	})
	if err != nil {
		return nil, err
	}
	content := artifact.authority.GetExpectedArchive().GetContent()
	if result.Committed.RecordSequence == 0 || result.Committed.NextOrdinal != content.EntryCount+1 {
		return nil, invalidRestoreArtifact()
	}
	return &agentpb.BackupConfigTransferCompleted{
		RestoreGenerationId:      artifact.authority.RestoreGenerationId,
		RenderGeneration:         artifact.authority.RenderGeneration,
		Content:                  proto.CloneOf(content),
		CommittedRecordCount:     result.Committed.RecordSequence,
		ValueChainSha256:         append([]byte(nil), result.Committed.ChainSHA256[:]...),
		TransferTranscriptSha256: append([]byte(nil), result.TranscriptSHA256[:]...),
	}, nil
}
