package backupconfig

import (
	"bytes"
	"context"
	"io"
)

const (
	manifestMemberName = "manifest.json"
	manifestMemberMode = uint32(0444)
)

// WriteArtifact writes one canonical USTAR to dst. It writes selected values
// directly from the supplied readers at their final offsets and never creates
// a second plaintext spool.
func WriteArtifact(
	ctx context.Context,
	dst io.WriterAt,
	manifest []byte,
	layout Layout,
	metadata []MetadataFrame,
	values []io.Reader,
) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if dst == nil {
		return archiveError("artifact destination is nil")
	}
	canonical, authority, err := buildManifestPayload(ctx, layout.Entries)
	if err != nil {
		return err
	}
	defer clearBytes(canonical)
	if !bytes.Equal(manifest, canonical) || authority != layout.Authority {
		return archiveError("artifact inputs do not match canonical content authority")
	}
	if err := validateMetadataFrames(layout.Entries, metadata); err != nil {
		return err
	}
	expectedLayout, err := ComputeLayout(ctx, authority, layout.Entries)
	if err != nil {
		return err
	}
	if !sameOffsets(layout, expectedLayout) || len(values) != len(layout.Entries) {
		return archiveError("artifact layout or selected value reader count is invalid")
	}

	header, err := canonicalHeader(manifestMemberName, manifestMemberMode, uint64(len(manifest)))
	if err != nil {
		return err
	}
	if err := writeAtFull(ctx, dst, header[:], 0); err != nil {
		return err
	}
	if err := writeAtFull(ctx, dst, manifest, TarBlockBytes); err != nil {
		return err
	}
	manifestRounded, _ := roundTar(uint64(len(manifest)))
	if err := writeZerosAt(
		ctx,
		dst,
		TarBlockBytes+uint64(len(manifest)),
		manifestRounded-uint64(len(manifest)),
	); err != nil {
		return err
	}

	for index, entry := range layout.Entries {
		if err := checkContext(ctx); err != nil {
			return err
		}
		mode := uint32(0444)
		if entry.Secret {
			mode = 0600
		}
		header, headerErr := canonicalHeader(entry.Value.Path, mode, entry.Value.SizeBytes)
		if headerErr != nil {
			return headerErr
		}
		if err := writeAtFull(ctx, dst, header[:], layout.ValueHeaderOffsets[index]); err != nil {
			return err
		}
		digest, streamErr := streamSelectedValue(
			ctx,
			values[index],
			entry,
			true,
			func(chunk []byte, offset uint64) error {
				return writeAtFull(ctx, dst, chunk, layout.ValuePayloadOffsets[index]+offset)
			},
		)
		if streamErr != nil {
			return streamErr
		}
		if digest != entry.Value.SHA256 {
			return archiveError("selected value digest does not match manifest evidence")
		}
		rounded, _ := roundTar(entry.Value.SizeBytes)
		if err := writeZerosAt(
			ctx,
			dst,
			layout.ValuePayloadOffsets[index]+entry.Value.SizeBytes,
			rounded-entry.Value.SizeBytes,
		); err != nil {
			return err
		}
	}
	return writeZerosAt(ctx, dst, layout.FooterOffset, 2*TarBlockBytes)
}

func writeAtFull(ctx context.Context, destination io.WriterAt, value []byte, offset uint64) error {
	for len(value) != 0 {
		if err := checkContext(ctx); err != nil {
			return err
		}
		count, err := destination.WriteAt(value, int64(offset))
		if count > 0 {
			offset += uint64(count)
			value = value[count:]
		}
		if err != nil {
			return archiveCause("artifact destination write failed", err)
		}
		if count == 0 {
			return archiveError("artifact destination write was short")
		}
	}
	return nil
}

func writeZerosAt(ctx context.Context, destination io.WriterAt, offset, length uint64) error {
	var zero [TarBlockBytes]byte
	for length != 0 {
		count := uint64(len(zero))
		if count > length {
			count = length
		}
		if err := writeAtFull(ctx, destination, zero[:int(count)], offset); err != nil {
			return err
		}
		offset += count
		length -= count
	}
	return nil
}
