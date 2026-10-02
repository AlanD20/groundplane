package backupconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"hash"
	"io"
)

// ArtifactEvidence authenticates every emitted USTAR byte, including headers,
// padding and the footer. It does not prove durable storage or publication.
type ArtifactEvidence struct {
	SizeBytes uint64
	SHA256    [sha256.Size]byte
}

// ArtifactWriter constructs the canonical archive directly in its destination.
// It retains metadata and at most an incomplete UTF-8 rune, never value bodies.
// A failed write poisons the construction; callers must not publish its output.
type ArtifactWriter struct {
	destination  io.Writer
	layout       Layout
	sourceHash   hash.Hash
	valueHash    hash.Hash
	validator    selectedValueValidator
	written      uint64
	ordinal      uint32
	valueOffset  uint64
	valueStarted bool
	failed       bool
	finished     bool
}

func NewArtifactWriter(
	ctx context.Context,
	destination io.Writer,
	manifest []byte,
	layout Layout,
	metadata []MetadataFrame,
) (*ArtifactWriter, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if destination == nil {
		return nil, archiveError("artifact destination is nil")
	}
	canonical, authority, err := buildManifestPayload(ctx, layout.Entries)
	if err != nil {
		return nil, err
	}
	defer clearBytes(canonical)
	if !bytes.Equal(manifest, canonical) || authority != layout.Authority {
		return nil, archiveError("artifact inputs do not match canonical content authority")
	}
	if err := validateMetadataFrames(layout.Entries, metadata); err != nil {
		return nil, err
	}
	expected, err := ComputeLayout(ctx, authority, layout.Entries)
	if err != nil {
		return nil, err
	}
	if !sameOffsets(layout, expected) {
		return nil, archiveError("artifact layout is invalid")
	}
	writer := &ArtifactWriter{destination: destination, layout: expected, sourceHash: sha256.New(), ordinal: 1}
	header, err := canonicalHeader(manifestMemberName, manifestMemberMode, uint64(len(manifest)))
	if err != nil {
		return nil, err
	}
	if err := writer.write(ctx, header[:]); err != nil {
		return nil, err
	}
	if err := writer.write(ctx, manifest); err != nil {
		return nil, err
	}
	rounded, _ := roundTar(uint64(len(manifest)))
	if err := writer.zeros(ctx, rounded-uint64(len(manifest))); err != nil {
		return nil, err
	}
	return writer, nil
}

// WriteValueChunk accepts only the next canonical chunk of the current Entry.
func (writer *ArtifactWriter) WriteValueChunk(
	ctx context.Context,
	ordinal uint32,
	offset uint64,
	content []byte,
) error {
	if err := writer.check(ctx, ordinal); err != nil {
		return err
	}
	entry := writer.layout.Entries[ordinal-1]
	remaining := entry.Value.SizeBytes - writer.valueOffset
	length := uint64(TransferChunkBytes)
	if length > remaining {
		length = remaining
	}
	if offset != writer.valueOffset || length == 0 || uint64(len(content)) != length {
		return writer.fail(archiveError("artifact value chunk is out of order or incorrectly sized"))
	}
	if err := writer.beginValue(ctx); err != nil {
		return err
	}
	if !writer.validator.consume(content) {
		return writer.fail(archiveError("selected value violates its UTF-8 or NUL policy"))
	}
	if err := writer.write(ctx, content); err != nil {
		return err
	}
	_, _ = writer.valueHash.Write(content)
	writer.valueOffset += uint64(len(content))
	return nil
}

// EndValue proves the complete selected value before admitting another Entry.
func (writer *ArtifactWriter) EndValue(ctx context.Context, ordinal uint32) error {
	if err := writer.check(ctx, ordinal); err != nil {
		return err
	}
	entry := writer.layout.Entries[ordinal-1]
	if writer.valueOffset != entry.Value.SizeBytes {
		return writer.fail(archiveError("artifact selected value is incomplete"))
	}
	if err := writer.beginValue(ctx); err != nil {
		return err
	}
	var digest [sha256.Size]byte
	copy(digest[:], writer.valueHash.Sum(nil))
	if !writer.validator.finish() || digest != entry.Value.SHA256 {
		return writer.fail(archiveError("artifact selected value content does not match its authority"))
	}
	rounded, _ := roundTar(entry.Value.SizeBytes)
	if err := writer.zeros(ctx, rounded-entry.Value.SizeBytes); err != nil {
		return err
	}
	writer.validator.clear()
	writer.valueHash = nil
	writer.valueStarted = false
	writer.valueOffset = 0
	writer.ordinal++
	return nil
}

func (writer *ArtifactWriter) Finish(ctx context.Context) (ArtifactEvidence, error) {
	if writer == nil || writer.failed || writer.finished || writer.valueStarted ||
		writer.ordinal != writer.layout.Authority.EntryCount+1 || writer.written != writer.layout.FooterOffset {
		return ArtifactEvidence{}, archiveError("artifact construction is not complete")
	}
	if err := writer.zeros(ctx, 2*TarBlockBytes); err != nil {
		return ArtifactEvidence{}, err
	}
	if writer.written != writer.layout.Authority.SourceSizeBytes {
		return ArtifactEvidence{}, writer.fail(archiveError("artifact size does not match its authority"))
	}
	writer.finished = true
	evidence := ArtifactEvidence{SizeBytes: writer.written}
	copy(evidence.SHA256[:], writer.sourceHash.Sum(nil))
	return evidence, nil
}

// Abort invalidates construction and clears any buffered UTF-8 bytes. The
// destination's owner remains responsible for closing and disposing its stage.
func (writer *ArtifactWriter) Abort() {
	if writer != nil {
		writer.failed = true
		writer.validator.clear()
	}
}

func (writer *ArtifactWriter) check(ctx context.Context, ordinal uint32) error {
	if writer == nil {
		return archiveError("artifact writer is nil")
	}
	if err := checkContext(ctx); err != nil {
		return writer.fail(err)
	}
	if writer.failed || writer.finished || ordinal != writer.ordinal || ordinal == 0 ||
		ordinal > writer.layout.Authority.EntryCount {
		return writer.fail(archiveError("artifact Entry cursor is invalid"))
	}
	return nil
}

func (writer *ArtifactWriter) beginValue(ctx context.Context) error {
	if writer.valueStarted {
		return nil
	}
	entry := writer.layout.Entries[writer.ordinal-1]
	if writer.written != writer.layout.ValueHeaderOffsets[writer.ordinal-1] {
		return writer.fail(archiveError("artifact value header offset is invalid"))
	}
	mode := uint32(0444)
	if entry.Secret {
		mode = 0600
	}
	header, err := canonicalHeader(entry.Value.Path, mode, entry.Value.SizeBytes)
	if err != nil {
		return writer.fail(err)
	}
	if err := writer.write(ctx, header[:]); err != nil {
		return err
	}
	writer.valueHash = sha256.New()
	writer.validator = selectedValueValidator{requireUTF8: entry.Source.Kind == SourceLiteral ||
		entry.Metadata.Kind == MetadataEnvironment, rejectNUL: entry.Metadata.Kind == MetadataEnvironment}
	writer.valueStarted = true
	return nil
}

func (writer *ArtifactWriter) write(ctx context.Context, content []byte) error {
	for len(content) > 0 {
		if err := checkContext(ctx); err != nil {
			return writer.fail(err)
		}
		count, err := writer.destination.Write(content)
		if count < 0 || count > len(content) {
			return writer.fail(archiveError("artifact destination returned an invalid write count"))
		}
		if count > 0 {
			_, _ = writer.sourceHash.Write(content[:count])
			writer.written += uint64(count)
			content = content[count:]
		}
		if err != nil {
			return writer.fail(archiveCause("artifact destination write failed", err))
		}
		if count == 0 {
			return writer.fail(archiveError("artifact destination write was short"))
		}
	}
	return nil
}

func (writer *ArtifactWriter) zeros(ctx context.Context, length uint64) error {
	var zero [TarBlockBytes]byte
	for length > 0 {
		count := uint64(len(zero))
		if count > length {
			count = length
		}
		if err := writer.write(ctx, zero[:int(count)]); err != nil {
			return err
		}
		length -= count
	}
	return nil
}

func (writer *ArtifactWriter) fail(err error) error {
	writer.failed = true
	writer.validator.clear()
	return err
}
