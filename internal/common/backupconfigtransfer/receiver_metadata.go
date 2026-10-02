package backupconfigtransfer

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (receiver *Receiver) sealMetadata(ctx context.Context) error {
	direction := backupconfig.TransferCapture
	if receiver.binding.Direction == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
		direction = backupconfig.TransferRestore
	}
	encode := func(ctx context.Context, requested backupconfig.TransferDirection, ordinal uint32, entry backupconfig.Entry) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if requested != direction || ordinal == 0 || int(ordinal) > len(receiver.headers) ||
			receiver.entries[ordinal-1].ID != entry.ID {
			return nil, invalid("Config metadata encoding cursor is invalid")
		}
		header := receiver.headers[ordinal-1]
		if direction == backupconfig.TransferCapture {
			return proto.MarshalOptions{Deterministic: true}.Marshal(header.GetCaptureEntry())
		}
		return proto.MarshalOptions{Deterministic: true}.Marshal(header.GetRestoreEntry())
	}
	manifest, content, metadata, err := backupconfig.BuildManifest(ctx, direction, receiver.entries, encode)
	if err != nil {
		return err
	}
	defer clear(manifest)
	defer func() {
		for _, frame := range metadata {
			clear(frame.CanonicalEntry)
		}
	}()
	if !sameContent(content, receiver.expected) {
		return invalid("Config metadata does not reproduce the sealed content authority")
	}
	if direction == backupconfig.TransferCapture {
		digest, _, err := backupconfig.MetadataSnapshotSHA256(ctx, receiver.entries, metadata)
		if err != nil {
			return err
		}
		if !sameDigest(digest[:], receiver.expected.MetadataSnapshotSha256) {
			return invalid("Config capture metadata does not match the sealed snapshot digest")
		}
	}
	layout, err := backupconfig.ComputeLayout(ctx, content, receiver.entries)
	if err != nil {
		return err
	}
	values := make([]backupconfig.ValueFrame, len(receiver.entries))
	for index, entry := range receiver.entries {
		values[index] = backupconfig.ValueFrame{
			Ordinal:   uint32(index + 1),
			EntryID:   entry.ID,
			SizeBytes: entry.Value.SizeBytes,
		}
	}
	transcript, err := backupconfig.NewTransferTranscriptWriter(ctx, io.Discard, direction, content, metadata, values)
	if err != nil {
		return err
	}
	artifact, err := backupconfig.NewArtifactWriter(ctx, receiver.destination, manifest, layout, metadata)
	if err != nil {
		return err
	}
	receiver.artifact, receiver.transcript, receiver.layout = artifact, transcript, layout
	receiver.progress.MetadataTranscriptSHA256 = transcript.MetadataSHA256()
	receiver.progress.MetadataReady = true
	// Canonical metadata is now represented by the manifest and transcript hash;
	// retain no second wire inventory for the value phase.
	receiver.headers = nil
	return nil
}

func sameContent(actual backupconfig.ContentAuthority, expected *agentpb.BackupConfigContentAuthority) bool {
	return sameDigest(actual.ManifestSHA256[:], expected.ManifestSha256) &&
		actual.EntryCount == expected.EntryCount && actual.TotalSelectedValueBytes == expected.TotalSelectedValueBytes &&
		actual.ManifestSizeBytes == expected.ManifestSizeBytes && actual.SourceSizeBytes == expected.SourceSizeBytes
}
