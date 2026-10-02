package backupconfig

import (
	"context"
	"crypto/sha256"
	"hash"
	"io"
)

// TransferTranscriptWriter hashes the canonical metadata-first transcript as
// chunks arrive. It retains only value descriptors and counters, not plaintext.
type TransferTranscriptWriter struct {
	destination    io.Writer
	hasher         hash.Hash
	values         []ValueFrame
	metadataSHA256 [sha256.Size]byte
	ordinal        uint32
	offset         uint64
	started        bool
	failed         bool
	finished       bool
}

func NewTransferTranscriptWriter(
	ctx context.Context,
	destination io.Writer,
	direction TransferDirection,
	authority ContentAuthority,
	metadata []MetadataFrame,
	values []ValueFrame,
) (*TransferTranscriptWriter, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if destination == nil || (direction != TransferCapture && direction != TransferRestore) {
		return nil, archiveError("transfer transcript destination or direction is invalid")
	}
	if authority.EntryCount > MaxEntries || authority.ManifestSizeBytes < 47 ||
		authority.TotalSelectedValueBytes > MaxTotalSelectedValueBytes || authority.ManifestSizeBytes > MaxManifestBytes ||
		authority.SourceSizeBytes < 2*TarBlockBytes || authority.SourceSizeBytes > MaxSourceBytes ||
		len(metadata) != int(authority.EntryCount) || len(values) != int(authority.EntryCount) {
		return nil, archiveError("transfer transcript authority or frame counts are invalid")
	}
	if err := validateTranscriptFrames(authority, metadata, values); err != nil {
		return nil, err
	}
	writer := &TransferTranscriptWriter{hasher: sha256.New(), ordinal: 1, values: make([]ValueFrame, len(values))}
	for index, value := range values {
		writer.values[index] = ValueFrame{Ordinal: value.Ordinal, EntryID: value.EntryID, SizeBytes: value.SizeBytes}
	}
	writer.destination = io.MultiWriter(destination, writer.hasher)
	if err := writer.writeMetadata(ctx, direction, authority, metadata); err != nil {
		return nil, err
	}
	copy(writer.metadataSHA256[:], writer.hasher.Sum(nil))
	return writer, nil
}

func (writer *TransferTranscriptWriter) MetadataSHA256() [sha256.Size]byte {
	return writer.metadataSHA256
}

func (writer *TransferTranscriptWriter) WriteValueChunk(
	ctx context.Context,
	ordinal uint32,
	offset uint64,
	content []byte,
) error {
	if err := writer.check(ctx, ordinal); err != nil {
		return err
	}
	remaining := writer.values[ordinal-1].SizeBytes - writer.offset
	length := uint64(TransferChunkBytes)
	if length > remaining {
		length = remaining
	}
	if offset != writer.offset || length == 0 || uint64(len(content)) != length {
		return writer.fail(archiveError("transcript value chunk is out of order or incorrectly sized"))
	}
	if err := writer.beginValue(ctx); err != nil {
		return err
	}
	if err := writeBytes(ctx, writer.destination, content); err != nil {
		return writer.fail(err)
	}
	writer.offset += uint64(len(content))
	return nil
}

func (writer *TransferTranscriptWriter) EndValue(ctx context.Context, ordinal uint32) error {
	if err := writer.check(ctx, ordinal); err != nil {
		return err
	}
	if writer.offset != writer.values[ordinal-1].SizeBytes {
		return writer.fail(archiveError("transcript value is incomplete"))
	}
	if err := writer.beginValue(ctx); err != nil {
		return err
	}
	writer.ordinal++
	writer.offset = 0
	writer.started = false
	return nil
}

func (writer *TransferTranscriptWriter) Finish(ctx context.Context) ([sha256.Size]byte, error) {
	if writer == nil || writer.failed || writer.finished || writer.started ||
		int(writer.ordinal) != len(writer.values)+1 {
		return [sha256.Size]byte{}, archiveError("transcript is not complete")
	}
	if err := checkContext(ctx); err != nil {
		return [sha256.Size]byte{}, writer.fail(err)
	}
	writer.finished = true
	var digest [sha256.Size]byte
	copy(digest[:], writer.hasher.Sum(nil))
	return digest, nil
}

func (writer *TransferTranscriptWriter) check(ctx context.Context, ordinal uint32) error {
	if writer == nil {
		return archiveError("transcript writer is nil")
	}
	if err := checkContext(ctx); err != nil {
		return writer.fail(err)
	}
	if writer.failed || writer.finished || ordinal != writer.ordinal || ordinal == 0 ||
		int(ordinal) > len(writer.values) {
		return writer.fail(archiveError("transcript Entry cursor is invalid"))
	}
	return nil
}

func (writer *TransferTranscriptWriter) beginValue(ctx context.Context) error {
	if writer.started {
		return nil
	}
	value := writer.values[writer.ordinal-1]
	chunks := (value.SizeBytes + TransferChunkBytes - 1) / TransferChunkBytes
	if err := writeUint32(ctx, writer.destination, value.Ordinal); err != nil {
		return writer.fail(err)
	}
	if err := writeUint32(ctx, writer.destination, uint32(chunks)); err != nil {
		return writer.fail(err)
	}
	if err := writeUint64(ctx, writer.destination, value.SizeBytes); err != nil {
		return writer.fail(err)
	}
	writer.started = true
	return nil
}

func (writer *TransferTranscriptWriter) writeMetadata(
	ctx context.Context,
	direction TransferDirection,
	authority ContentAuthority,
	metadata []MetadataFrame,
) error {
	if err := writeBytes(ctx, writer.destination, []byte(transcriptDomain+"\x00")); err != nil {
		return err
	}
	if err := writeUint32(ctx, writer.destination, uint32(direction)); err != nil {
		return err
	}
	if err := writeBytes(ctx, writer.destination, authority.ManifestSHA256[:]); err != nil {
		return err
	}
	if err := writeUint64(ctx, writer.destination, authority.ManifestSizeBytes); err != nil {
		return err
	}
	if err := writeUint64(ctx, writer.destination, authority.SourceSizeBytes); err != nil {
		return err
	}
	if err := writeUint32(ctx, writer.destination, authority.EntryCount); err != nil {
		return err
	}
	if err := writeUint64(ctx, writer.destination, authority.TotalSelectedValueBytes); err != nil {
		return err
	}
	for _, frame := range metadata {
		if err := writeUint32(ctx, writer.destination, frame.Ordinal); err != nil {
			return err
		}
		if err := writeUint32(ctx, writer.destination, uint32(len(frame.CanonicalEntry))); err != nil {
			return err
		}
		if err := writeBytes(ctx, writer.destination, frame.CanonicalEntry); err != nil {
			return err
		}
	}
	return nil
}

func (writer *TransferTranscriptWriter) fail(err error) error { writer.failed = true; return err }
