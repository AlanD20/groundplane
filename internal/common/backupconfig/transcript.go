package backupconfig

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

const transcriptDomain = "groundplane.backup.config-transfer.v1"

type TransferDirection uint32

const (
	TransferCapture TransferDirection = 1
	TransferRestore TransferDirection = 2
)

// MetadataEncoder is implemented by the protocol layer using generated
// schema-1 types. The archive leaf never guesses protobuf tags or fields.
type MetadataEncoder func(context.Context, TransferDirection, uint32, Entry) ([]byte, error)

// MetadataFrame contains the deterministic, unknown-field-free protocol
// encoding supplied by the protocol layer. This package owns only its exact
// length framing and ordering.
type MetadataFrame struct {
	Ordinal        uint32
	EntryID        string
	CanonicalEntry []byte
}

type ValueFrame struct {
	Ordinal   uint32
	EntryID   string
	SizeBytes uint64
	Reader    io.Reader
}

// WriteTransferTranscript emits all metadata frames first, then all selected
// value frames, using the exact schema-1 domain and count/length framing.
func WriteTransferTranscript(
	ctx context.Context,
	destination io.Writer,
	direction TransferDirection,
	authority ContentAuthority,
	metadata []MetadataFrame,
	values []ValueFrame,
) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if destination == nil || (direction != TransferCapture && direction != TransferRestore) {
		return archiveError("transfer transcript destination or direction is invalid")
	}
	if authority.EntryCount > MaxEntries || authority.ManifestSizeBytes < 47 ||
		authority.TotalSelectedValueBytes > MaxTotalSelectedValueBytes ||
		authority.ManifestSizeBytes > MaxManifestBytes ||
		authority.SourceSizeBytes < 2*TarBlockBytes || authority.SourceSizeBytes > MaxSourceBytes ||
		len(metadata) != int(authority.EntryCount) || len(values) != int(authority.EntryCount) {
		return archiveError("transfer transcript authority or frame counts are invalid")
	}
	if err := validateTranscriptFrames(authority, metadata, values); err != nil {
		return err
	}
	if err := writeBytes(ctx, destination, []byte(transcriptDomain)); err != nil {
		return err
	}
	if err := writeBytes(ctx, destination, []byte{0}); err != nil {
		return err
	}
	if err := writeUint32(ctx, destination, uint32(direction)); err != nil {
		return err
	}
	if err := writeBytes(ctx, destination, authority.ManifestSHA256[:]); err != nil {
		return err
	}
	if err := writeUint64(ctx, destination, authority.ManifestSizeBytes); err != nil {
		return err
	}
	if err := writeUint64(ctx, destination, authority.SourceSizeBytes); err != nil {
		return err
	}
	if err := writeUint32(ctx, destination, authority.EntryCount); err != nil {
		return err
	}
	if err := writeUint64(ctx, destination, authority.TotalSelectedValueBytes); err != nil {
		return err
	}

	for _, frame := range metadata {
		if err := checkContext(ctx); err != nil {
			return err
		}
		if err := writeUint32(ctx, destination, frame.Ordinal); err != nil {
			return err
		}
		if err := writeUint32(ctx, destination, uint32(len(frame.CanonicalEntry))); err != nil {
			return err
		}
		if err := writeBytes(ctx, destination, frame.CanonicalEntry); err != nil {
			return err
		}
	}

	var total uint64
	buffer := make([]byte, TransferChunkBytes)
	defer clearBytes(buffer)
	for _, frame := range values {
		if err := checkContext(ctx); err != nil {
			return err
		}
		total += frame.SizeBytes
		chunkCount := frame.SizeBytes / TransferChunkBytes
		if frame.SizeBytes%TransferChunkBytes != 0 {
			chunkCount++
		}
		if err := writeUint32(ctx, destination, frame.Ordinal); err != nil {
			return err
		}
		if err := writeUint32(ctx, destination, uint32(chunkCount)); err != nil {
			return err
		}
		if err := writeUint64(ctx, destination, frame.SizeBytes); err != nil {
			return err
		}
		remaining := frame.SizeBytes
		for remaining != 0 {
			if err := checkContext(ctx); err != nil {
				return err
			}
			length := uint64(len(buffer))
			if length > remaining {
				length = remaining
			}
			chunk := buffer[:int(length)]
			if err := readFull(ctx, frame.Reader, chunk); err != nil {
				return err
			}
			if err := writeBytes(ctx, destination, chunk); err != nil {
				return err
			}
			remaining -= length
		}
		var extra [1]byte
		if err := checkContext(ctx); err != nil {
			return err
		}
		count, err := frame.Reader.Read(extra[:])
		if count != 0 || err == nil {
			return archiveError("transfer transcript value is longer than declared")
		}
		if err != io.EOF {
			return archiveCause("transfer transcript value EOF probe failed", err)
		}
	}
	if total != authority.TotalSelectedValueBytes {
		return archiveError("transfer transcript value total does not match authority")
	}
	return nil
}

func TransferTranscriptSHA256(
	ctx context.Context,
	direction TransferDirection,
	authority ContentAuthority,
	metadata []MetadataFrame,
	values []ValueFrame,
) ([32]byte, error) {
	hasher := sha256.New()
	if err := WriteTransferTranscript(ctx, hasher, direction, authority, metadata, values); err != nil {
		return [32]byte{}, err
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

func writeUint32(ctx context.Context, destination io.Writer, value uint32) error {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return writeBytes(ctx, destination, encoded[:])
}

func writeUint64(ctx context.Context, destination io.Writer, value uint64) error {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return writeBytes(ctx, destination, encoded[:])
}

func writeBytes(ctx context.Context, destination io.Writer, value []byte) error {
	for len(value) != 0 {
		if err := checkContext(ctx); err != nil {
			return err
		}
		count, err := destination.Write(value)
		if count > 0 {
			value = value[count:]
		}
		if err != nil {
			return archiveCause("transfer transcript write failed", err)
		}
		if count == 0 {
			return archiveError("transfer transcript write was short")
		}
	}
	return nil
}

func encodeMetadata(
	ctx context.Context,
	direction TransferDirection,
	entries []Entry,
	encoder MetadataEncoder,
) ([]MetadataFrame, error) {
	if direction != TransferCapture && direction != TransferRestore {
		return nil, archiveError("metadata direction is invalid")
	}
	if len(entries) != 0 && encoder == nil {
		return nil, archiveError("canonical protocol metadata encoder is required")
	}
	frames := make([]MetadataFrame, len(entries))
	for index, entry := range entries {
		if err := checkContext(ctx); err != nil {
			clearMetadataFrames(frames)
			return nil, err
		}
		encoded, err := encoder(ctx, direction, uint32(index+1), cloneEntries([]Entry{entry})[0])
		if err != nil {
			clearMetadataFrames(frames)
			return nil, archiveCause("canonical protocol metadata encoder failed", err)
		}
		if len(encoded) == 0 || len(encoded) > MaxCanonicalEntryBytes {
			clearMetadataFrames(frames)
			return nil, archiveError("canonical protocol metadata exceeds its per-Entry byte limit")
		}
		frames[index] = MetadataFrame{
			Ordinal: uint32(index + 1), EntryID: entry.ID, CanonicalEntry: append([]byte(nil), encoded...),
		}
	}
	if err := validateMetadataFrames(entries, frames); err != nil {
		clearMetadataFrames(frames)
		return nil, err
	}
	return frames, nil
}

func validateMetadataFrames(entries []Entry, frames []MetadataFrame) error {
	if len(entries) != len(frames) {
		return archiveError("canonical protocol metadata count does not match Entries")
	}
	var aggregate uint64
	for index, frame := range frames {
		if frame.Ordinal != uint32(index+1) || frame.EntryID != entries[index].ID ||
			len(frame.CanonicalEntry) == 0 || len(frame.CanonicalEntry) > MaxCanonicalEntryBytes {
			return archiveError("canonical protocol metadata is not bound to its Entry and ordinal")
		}
		aggregate += uint64(len(frame.CanonicalEntry))
		if aggregate > MaxDurableMetadataBytes {
			return archiveError("canonical protocol metadata aggregate exceeds its byte limit")
		}
	}
	return nil
}

func validateTranscriptFrames(authority ContentAuthority, metadata []MetadataFrame, values []ValueFrame) error {
	var aggregate uint64
	var total uint64
	memberBytes, ok := checkedAdd(uint64(authority.EntryCount), 1)
	if !ok || memberBytes > ^uint64(0)/TarBlockBytes {
		return archiveError("transfer transcript source geometry overflows")
	}
	memberBytes *= TarBlockBytes
	manifestRounded, ok := roundTar(authority.ManifestSizeBytes)
	if !ok {
		return archiveError("transfer transcript manifest geometry overflows")
	}
	sourceSize, ok := checkedAdd(2*TarBlockBytes, memberBytes)
	if !ok {
		return archiveError("transfer transcript source geometry overflows")
	}
	sourceSize, ok = checkedAdd(sourceSize, manifestRounded)
	if !ok {
		return archiveError("transfer transcript source geometry overflows")
	}
	for index := range metadata {
		frame := metadata[index]
		value := values[index]
		if frame.Ordinal != uint32(index+1) || value.Ordinal != frame.Ordinal ||
			ids.Validate(ids.KindEnvEntry, frame.EntryID) != nil || value.EntryID != frame.EntryID ||
			(index > 0 && metadata[index-1].EntryID >= frame.EntryID) ||
			len(frame.CanonicalEntry) == 0 || len(frame.CanonicalEntry) > MaxCanonicalEntryBytes ||
			value.SizeBytes > MaxSelectedValueBytes || value.Reader == nil {
			return archiveError("transfer transcript frame binding is invalid")
		}
		aggregate += uint64(len(frame.CanonicalEntry))
		if aggregate > MaxDurableMetadataBytes || total > MaxTotalSelectedValueBytes-value.SizeBytes {
			return archiveError("transfer transcript aggregate exceeds its byte limit")
		}
		total += value.SizeBytes
		rounded, roundOK := roundTar(value.SizeBytes)
		if !roundOK {
			return archiveError("transfer transcript value geometry overflows")
		}
		sourceSize, ok = checkedAdd(sourceSize, rounded)
		if !ok {
			return archiveError("transfer transcript source geometry overflows")
		}
	}
	if total != authority.TotalSelectedValueBytes || sourceSize != authority.SourceSizeBytes ||
		sourceSize > MaxSourceBytes {
		return archiveError("transfer transcript source geometry does not match authority")
	}
	return nil
}

func cloneMetadataFrames(frames []MetadataFrame) []MetadataFrame {
	result := make([]MetadataFrame, len(frames))
	copy(result, frames)
	for index := range result {
		result[index].CanonicalEntry = append([]byte(nil), frames[index].CanonicalEntry...)
	}
	return result
}

func clearMetadataFrames(frames []MetadataFrame) {
	for index := range frames {
		clearBytes(frames[index].CanonicalEntry)
	}
}
