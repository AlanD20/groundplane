package backupconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
)

func readFull(ctx context.Context, source io.Reader, destination []byte) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	_, err := io.ReadFull(source, destination)
	if err != nil {
		return archiveCause("artifact source read failed", err)
	}
	return checkContext(ctx)
}

func readFullContext(ctx context.Context, source io.Reader, destination []byte) error {
	for len(destination) != 0 {
		if err := checkContext(ctx); err != nil {
			return err
		}
		length := len(destination)
		if length > TransferChunkBytes {
			length = TransferChunkBytes
		}
		if err := readFull(ctx, source, destination[:length]); err != nil {
			return err
		}
		destination = destination[length:]
	}
	return nil
}

func readZeroBytes(ctx context.Context, source io.Reader, length uint64) error {
	var buffer [TarBlockBytes]byte
	for length != 0 {
		count := uint64(len(buffer))
		if count > length {
			count = length
		}
		chunk := buffer[:int(count)]
		if err := readFull(ctx, source, chunk); err != nil {
			return err
		}
		if !allZero(chunk) {
			return archiveError("nonzero byte appears in canonical zero region")
		}
		length -= count
	}
	return nil
}

func allZero(value []byte) bool {
	for _, character := range value {
		if character != 0 {
			return false
		}
	}
	return true
}

func sameOffsets(left, right Layout) bool {
	if left.FooterOffset != right.FooterOffset ||
		len(left.ValueHeaderOffsets) != len(right.ValueHeaderOffsets) ||
		len(left.ValuePayloadOffsets) != len(right.ValuePayloadOffsets) {
		return false
	}
	for index := range left.ValueHeaderOffsets {
		if left.ValueHeaderOffsets[index] != right.ValueHeaderOffsets[index] ||
			left.ValuePayloadOffsets[index] != right.ValuePayloadOffsets[index] {
			return false
		}
	}
	return true
}

func sameLayoutContent(ctx context.Context, left, right Layout) (bool, error) {
	if left.Authority != right.Authority || !sameOffsets(left, right) ||
		len(left.Entries) != len(right.Entries) {
		return false, nil
	}
	leftManifest, leftAuthority, err := buildManifestPayload(ctx, left.Entries)
	if err != nil {
		return false, err
	}
	defer clearBytes(leftManifest)
	rightManifest, rightAuthority, err := buildManifestPayload(ctx, right.Entries)
	if err != nil {
		return false, err
	}
	defer clearBytes(rightManifest)
	return leftAuthority == rightAuthority && bytes.Equal(leftManifest, rightManifest), nil
}

func probeExactEOF(ctx context.Context, source io.ReaderAt, exactSize uint64) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	var extra [1]byte
	count, err := source.ReadAt(extra[:], int64(exactSize))
	if count != 0 || err == nil {
		return archiveError(
			"artifact source contains a hidden trailing byte or cannot prove exact EOF",
		)
	}
	if err != io.EOF {
		return archiveCause("artifact source EOF probe failed", err)
	}
	return nil
}

func proveOwnedExactSize(ctx context.Context, source ownedSpool, exactSize uint64) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	info, err := source.Stat()
	if err != nil {
		return archiveCause("private validated spool fstat failed", err)
	}
	if info.Size() < 0 || uint64(info.Size()) != exactSize {
		return archiveError("private validated spool length differs from mandatory source size")
	}
	return probeExactEOF(ctx, source, exactSize)
}

func copyOwnedSource(
	ctx context.Context,
	destination ownedSpool,
	source io.ReaderAt,
	evidence SourceEvidence,
) error {
	hasher := sha256.New()
	buffer := make([]byte, TransferChunkBytes)
	defer clearBytes(buffer)
	var offset uint64
	for offset < evidence.SizeBytes {
		if err := checkContext(ctx); err != nil {
			return err
		}
		length := uint64(len(buffer))
		if remaining := evidence.SizeBytes - offset; length > remaining {
			length = remaining
		}
		chunk := buffer[:int(length)]
		if err := readAtContext(ctx, source, chunk, offset); err != nil {
			return err
		}
		// hash.Hash.Write is specified to consume all bytes and never return an error.
		_, _ = hasher.Write(chunk)
		if err := writeFileContext(ctx, destination, chunk); err != nil {
			return err
		}
		offset += length
	}
	if err := probeExactEOF(ctx, source, evidence.SizeBytes); err != nil {
		return err
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	if digest != evidence.SHA256 {
		return archiveError("artifact source digest does not match mandatory evidence")
	}
	return nil
}

func readAtContext(
	ctx context.Context,
	source io.ReaderAt,
	destination []byte,
	offset uint64,
) error {
	for len(destination) != 0 {
		if err := checkContext(ctx); err != nil {
			return err
		}
		requested := len(destination)
		count, err := source.ReadAt(destination, int64(offset))
		if count > 0 {
			offset += uint64(count)
			destination = destination[count:]
		}
		if err != nil && !(err == io.EOF && count == requested) {
			return archiveCause("artifact source read failed", err)
		}
		if count == 0 {
			return archiveError("artifact source read was short")
		}
	}
	return nil
}

func writeFileContext(ctx context.Context, destination io.Writer, value []byte) error {
	for len(value) != 0 {
		if err := checkContext(ctx); err != nil {
			return err
		}
		count, err := destination.Write(value)
		if count > 0 {
			value = value[count:]
		}
		if err != nil {
			return archiveCause("private validated spool write failed", err)
		}
		if count == 0 {
			return archiveError("private validated spool write was short")
		}
	}
	return nil
}

func cloneLayout(layout Layout) Layout {
	return Layout{
		Authority:           layout.Authority,
		Entries:             cloneEntries(layout.Entries),
		ValueHeaderOffsets:  append([]uint64(nil), layout.ValueHeaderOffsets...),
		ValuePayloadOffsets: append([]uint64(nil), layout.ValuePayloadOffsets...),
		FooterOffset:        layout.FooterOffset,
	}
}
