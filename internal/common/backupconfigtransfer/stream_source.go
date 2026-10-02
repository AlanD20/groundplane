package backupconfigtransfer

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type streamSource struct {
	content   *agentpb.BackupConfigContentAuthority
	headers   []*agentpb.BackupConfigEntryHeader
	entries   []backupconfig.Entry
	authority backupconfig.ContentAuthority
	metadata  []backupconfig.MetadataFrame
}

func prepareStreamSource(ctx context.Context, direction agentpb.BackupConfigDirection,
	expected *agentpb.BackupConfigContentAuthority, headers []*agentpb.BackupConfigEntryHeader,
) (_ *streamSource, resultErr error) {
	if !validDirection(direction) || !validContent(expected) || rejectUnknown(expected) != nil ||
		len(headers) != int(expected.EntryCount) {
		return nil, invalidStream()
	}
	source := &streamSource{content: proto.CloneOf(expected),
		headers: make([]*agentpb.BackupConfigEntryHeader, len(headers)),
		entries: make([]backupconfig.Entry, len(headers))}
	keep := false
	defer func() {
		if !keep {
			source.clear()
		}
	}()
	for index, header := range headers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if header == nil || rejectUnknown(header) != nil {
			return nil, invalidStream()
		}
		owned := proto.CloneOf(header)
		entry, err := entryFromHeader(direction, owned)
		if err != nil {
			return nil, err
		}
		if owned.ChunkCount != uint32(
			(entry.Value.SizeBytes+backupconfig.TransferChunkBytes-1)/backupconfig.TransferChunkBytes,
		) ||
			(owned.GetCaptureEntry() != nil && owned.GetCaptureEntry().Ordinal != uint32(index+1)) {
			return nil, invalidStream()
		}
		source.headers[index], source.entries[index] = owned, entry
	}
	archiveDirection := backupconfig.TransferCapture
	if direction == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
		archiveDirection = backupconfig.TransferRestore
	}
	manifest, content, metadata, err := backupconfig.BuildManifest(
		ctx,
		archiveDirection,
		source.entries,
		func(ctx context.Context, requested backupconfig.TransferDirection, ordinal uint32, entry backupconfig.Entry) ([]byte, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if requested != archiveDirection || ordinal == 0 || int(ordinal) > len(source.headers) ||
				entry.ID != source.entries[ordinal-1].ID {
				return nil, invalidStream()
			}
			header := source.headers[ordinal-1]
			if archiveDirection == backupconfig.TransferCapture {
				return proto.MarshalOptions{Deterministic: true}.Marshal(header.GetCaptureEntry())
			}
			return proto.MarshalOptions{Deterministic: true}.Marshal(header.GetRestoreEntry())
		},
	)
	if err != nil {
		return nil, err
	}
	defer clear(manifest)
	source.authority, source.metadata = content, metadata
	if !sameContent(content, source.content) {
		return nil, invalidStream()
	}
	if archiveDirection == backupconfig.TransferCapture {
		digest, _, err := backupconfig.MetadataSnapshotSHA256(ctx, source.entries, metadata)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(digest[:], source.content.MetadataSnapshotSha256) {
			return nil, invalidStream()
		}
	}
	keep = true
	return source, nil
}

func (source *streamSource) clear() {
	for _, metadata := range source.metadata {
		clear(metadata.CanonicalEntry)
	}
	source.metadata = nil
	source.headers = nil
	source.entries = nil
}
