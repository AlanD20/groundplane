package backupconfig

import (
	"context"
	"io"
)

const (
	manifestMemberName = "manifest.json"
	manifestMemberMode = uint32(0444)
)

// WriteArtifact writes one canonical USTAR at contiguous final offsets. Both
// reader-based capture and framed channel capture use ArtifactWriter's single
// construction and selected-value validation path.
func WriteArtifact(
	ctx context.Context,
	dst io.WriterAt,
	manifest []byte,
	layout Layout,
	metadata []MetadataFrame,
	values []io.Reader,
) error {
	if dst == nil || len(values) != len(layout.Entries) {
		return archiveError("artifact destination or value reader count is invalid")
	}
	writer, err := NewArtifactWriter(ctx, &contiguousWriterAt{destination: dst}, manifest, layout, metadata)
	if err != nil {
		return err
	}
	buffer := make([]byte, TransferChunkBytes)
	defer clearBytes(buffer)
	for index, entry := range layout.Entries {
		if values[index] == nil {
			return writer.fail(archiveError("selected value reader is nil"))
		}
		var offset uint64
		for offset < entry.Value.SizeBytes {
			length := uint64(len(buffer))
			if length > entry.Value.SizeBytes-offset {
				length = entry.Value.SizeBytes - offset
			}
			chunk := buffer[:int(length)]
			if err := readFull(ctx, values[index], chunk); err != nil {
				return writer.fail(err)
			}
			if err := writer.WriteValueChunk(ctx, uint32(index+1), offset, chunk); err != nil {
				return err
			}
			offset += length
		}
		if err := checkContext(ctx); err != nil {
			return writer.fail(err)
		}
		var extra [1]byte
		count, err := values[index].Read(extra[:])
		clearBytes(extra[:])
		if count != 0 || err == nil {
			return writer.fail(archiveError("selected value is longer than its declared size"))
		}
		if err != io.EOF {
			return writer.fail(archiveCause("selected value EOF probe failed", err))
		}
		if err := writer.EndValue(ctx, uint32(index+1)); err != nil {
			return err
		}
	}
	_, err = writer.Finish(ctx)
	return err
}

type contiguousWriterAt struct {
	destination io.WriterAt
	offset      int64
}

func (writer *contiguousWriterAt) Write(content []byte) (int, error) {
	count, err := writer.destination.WriteAt(content, writer.offset)
	if count > 0 && count <= len(content) {
		writer.offset += int64(count)
	}
	return count, err
}
