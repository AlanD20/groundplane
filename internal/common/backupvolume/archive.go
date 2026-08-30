package backupvolume

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"hash"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
)

// Write emits one canonical archive and verifies every regular-file reader
// against its declared exact length, EOF, and content digest.
func Write(
	ctx context.Context,
	destination io.Writer,
	entries []Entry,
	contents []io.Reader,
	manifest []ManifestEntryBytes,
) (ArtifactEvidence, error) {
	if destination == nil {
		return ArtifactEvidence{}, invalid("volume archive destination is required")
	}
	if len(contents) != len(entries) {
		return ArtifactEvidence{}, invalid("volume archive content reader count does not match")
	}
	sourceSize, err := archiveSize(entries)
	if err != nil {
		return ArtifactEvidence{}, err
	}
	if err := ValidateManifest(manifest, len(entries)); err != nil {
		return ArtifactEvidence{}, err
	}
	contentManifest, err := ContentManifestSHA256(manifest)
	if err != nil {
		return ArtifactEvidence{}, err
	}
	fullTree, err := FullTreeSHA256(entries)
	if err != nil {
		return ArtifactEvidence{}, err
	}
	writer := archiveWriter{destination: destination, digest: sha256.New()}
	for index, entry := range entries {
		plan, planErr := planHeader(entry, uint64(index))
		if planErr != nil {
			return ArtifactEvidence{}, planErr
		}
		if plan.hasPAX {
			if err := writer.write(ctx, plan.paxHeader[:]); err != nil {
				return ArtifactEvidence{}, err
			}
			if err := writer.write(ctx, plan.paxPayload); err != nil {
				return ArtifactEvidence{}, err
			}
			padding := rounded(uint64(len(plan.paxPayload))) - uint64(len(plan.paxPayload))
			if err := writer.zeros(ctx, padding); err != nil {
				return ArtifactEvidence{}, err
			}
		}
		if err := writer.write(ctx, plan.memberHeader[:]); err != nil {
			return ArtifactEvidence{}, err
		}
		if entry.Kind == EntryDirectory {
			if contents[index] != nil {
				return ArtifactEvidence{}, invalid("volume archive directory has a content reader")
			}
			continue
		}
		if contents[index] == nil {
			return ArtifactEvidence{}, invalid("volume archive regular file has no content reader")
		}
		if err := writer.file(ctx, contents[index], entry); err != nil {
			return ArtifactEvidence{}, err
		}
		if err := writer.zeros(ctx, rounded(entry.SizeBytes)-entry.SizeBytes); err != nil {
			return ArtifactEvidence{}, err
		}
	}
	if err := writer.zeros(ctx, 2*TarBlockBytes); err != nil {
		return ArtifactEvidence{}, err
	}
	if writer.count != sourceSize {
		return ArtifactEvidence{}, internal("volume archive writer produced an unexpected size")
	}
	var sourceDigest [sha256.Size]byte
	copy(sourceDigest[:], writer.digest.Sum(nil))
	return ArtifactEvidence{
		Source: backupformat.Evidence{SizeBytes: sourceSize, SHA256: sourceDigest},
		Archive: ArchiveEvidence{
			EntryCount:            uint64(len(entries)),
			ContentManifestSHA256: contentManifest,
			FullTreeSHA256:        fullTree,
			SourceSizeBytes:       sourceSize,
		},
	}, nil
}

// Validate consumes the complete sealed stream and accepts only byte-for-byte
// canonical USTAR/PAX plus exact source and format-specific evidence.
func Validate(
	ctx context.Context,
	source io.Reader,
	manifest []ManifestEntryBytes,
	expected ArtifactEvidence,
) (ValidatedArchive, error) {
	if source == nil {
		return ValidatedArchive{}, invalid("volume archive source is required")
	}
	if err := expected.Validate(); err != nil {
		return ValidatedArchive{}, err
	}
	if err := ValidateManifest(manifest, int(expected.Archive.EntryCount)); err != nil {
		return ValidatedArchive{}, err
	}
	reader := archiveReader{source: source, remaining: expected.Source.SizeBytes, digest: sha256.New()}
	entries := make([]Entry, 0, expected.Archive.EntryCount)
	for {
		var first [TarBlockBytes]byte
		if err := reader.read(ctx, first[:]); err != nil {
			return ValidatedArchive{}, err
		}
		if allZero(first[:]) {
			var second [TarBlockBytes]byte
			if err := reader.read(ctx, second[:]); err != nil {
				return ValidatedArchive{}, err
			}
			if !allZero(second[:]) {
				return ValidatedArchive{}, invalid("volume archive footer is nonzero")
			}
			break
		}
		firstHeader, err := parseHeader(first)
		if err != nil {
			return ValidatedArchive{}, err
		}
		var pax *paxValues
		var paxPayload []byte
		var memberBlock [TarBlockBytes]byte
		memberHeader := firstHeader
		if firstHeader.typeflag == 'x' {
			if firstHeader.size == 0 || firstHeader.size > maxPAXPayload {
				return ValidatedArchive{}, invalid("volume PAX payload length is invalid")
			}
			paxPayload = make([]byte, int(firstHeader.size))
			if err := reader.read(ctx, paxPayload); err != nil {
				return ValidatedArchive{}, err
			}
			if err := reader.zeroPadding(ctx, firstHeader.size); err != nil {
				return ValidatedArchive{}, err
			}
			parsed, parseErr := parsePAX(paxPayload)
			if parseErr != nil {
				return ValidatedArchive{}, parseErr
			}
			pax = &parsed
			if err := reader.read(ctx, memberBlock[:]); err != nil {
				return ValidatedArchive{}, err
			}
			if allZero(memberBlock[:]) {
				return ValidatedArchive{}, invalid("volume PAX header is unconsumed")
			}
			memberHeader, err = parseHeader(memberBlock)
			if err != nil {
				return ValidatedArchive{}, err
			}
			if memberHeader.typeflag == 'x' {
				return ValidatedArchive{}, invalid("volume PAX headers cannot be chained")
			}
		} else {
			memberBlock = first
		}
		entry, err := entryFromHeader(memberHeader, pax)
		if err != nil {
			return ValidatedArchive{}, err
		}
		if err := validateEntryShape(entry, len(entries)); err != nil {
			return ValidatedArchive{}, err
		}
		if len(entries) > 1 && bytes.Compare(entries[len(entries)-1].Path, entry.Path) >= 0 {
			return ValidatedArchive{}, invalid("volume archive paths are duplicated or out of order")
		}
		plan, err := planHeader(entry, uint64(len(entries)))
		if err != nil {
			return ValidatedArchive{}, err
		}
		if plan.hasPAX != (pax != nil) || !bytes.Equal(plan.memberHeader[:], memberBlock[:]) {
			return ValidatedArchive{}, invalid("volume archive member header is not canonical")
		}
		if pax != nil && (!bytes.Equal(plan.paxHeader[:], first[:]) || !bytes.Equal(plan.paxPayload, paxPayload)) {
			return ValidatedArchive{}, invalid("volume PAX framing is not canonical")
		}
		if entry.Kind == EntryRegular {
			entry.ContentSHA256, err = reader.file(ctx, entry.SizeBytes)
			if err != nil {
				return ValidatedArchive{}, err
			}
			if err := reader.zeroPadding(ctx, entry.SizeBytes); err != nil {
				return ValidatedArchive{}, err
			}
		}
		entries = append(entries, entry)
		if len(entries) > MaxEntries {
			return ValidatedArchive{}, invalid("volume archive exceeds its entry limit")
		}
	}
	if reader.remaining != 0 {
		return ValidatedArchive{}, invalid("volume archive has additional footer or trailing bytes")
	}
	if err := reader.exactEOF(); err != nil {
		return ValidatedArchive{}, err
	}
	if subtle.ConstantTimeCompare(reader.digest.Sum(nil), expected.Source.SHA256[:]) != 1 {
		return ValidatedArchive{}, invalid("volume archive source digest does not match")
	}
	if err := ValidateEntries(entries); err != nil {
		return ValidatedArchive{}, err
	}
	manifestDigest, err := ContentManifestSHA256(manifest)
	if err != nil {
		return ValidatedArchive{}, err
	}
	treeDigest, err := FullTreeSHA256(entries)
	if err != nil {
		return ValidatedArchive{}, err
	}
	if uint64(len(entries)) != expected.Archive.EntryCount ||
		subtle.ConstantTimeCompare(manifestDigest[:], expected.Archive.ContentManifestSHA256[:]) != 1 ||
		subtle.ConstantTimeCompare(treeDigest[:], expected.Archive.FullTreeSHA256[:]) != 1 {
		return ValidatedArchive{}, invalid("volume archive manifest evidence does not match")
	}
	return ValidatedArchive{Entries: cloneEntries(entries), Evidence: expected}, nil
}

type archiveWriter struct {
	destination io.Writer
	digest      hash.Hash
	count       uint64
}

func (writer *archiveWriter) write(ctx context.Context, value []byte) error {
	if ctx.Err() != nil {
		return internal("volume archive write was canceled")
	}
	written := 0
	for written < len(value) {
		count, err := writer.destination.Write(value[written:])
		if count > 0 {
			_, _ = writer.digest.Write(value[written : written+count])
			writer.count += uint64(count)
			written += count
		}
		if err != nil {
			return internal("volume archive destination write failed")
		}
		if count == 0 {
			return internal("volume archive destination made no progress")
		}
	}
	return nil
}

func (writer *archiveWriter) zeros(ctx context.Context, count uint64) error {
	var zero [TarBlockBytes]byte
	for count > 0 {
		length := count
		if length > TarBlockBytes {
			length = TarBlockBytes
		}
		if err := writer.write(ctx, zero[:int(length)]); err != nil {
			return err
		}
		count -= length
	}
	return nil
}

func (writer *archiveWriter) file(ctx context.Context, source io.Reader, entry Entry) error {
	digest := sha256.New()
	buffer := make([]byte, 32*1024)
	remaining := entry.SizeBytes
	for remaining > 0 {
		want := uint64(len(buffer))
		if remaining < want {
			want = remaining
		}
		count, err := io.ReadFull(source, buffer[:int(want)])
		if err != nil || count != int(want) {
			return invalid("volume archive file content is truncated")
		}
		_, _ = digest.Write(buffer[:count])
		if err := writer.write(ctx, buffer[:count]); err != nil {
			return err
		}
		remaining -= uint64(count)
	}
	var extra [1]byte
	count, err := io.ReadFull(source, extra[:])
	if count != 0 || err != io.EOF {
		return invalid("volume archive file content has trailing bytes")
	}
	if subtle.ConstantTimeCompare(digest.Sum(nil), entry.ContentSHA256[:]) != 1 {
		return invalid("volume archive file content digest does not match")
	}
	return nil
}

type archiveReader struct {
	source    io.Reader
	remaining uint64
	digest    hash.Hash
}

func (reader *archiveReader) read(ctx context.Context, destination []byte) error {
	if ctx.Err() != nil {
		return invalid("volume archive validation was canceled")
	}
	if uint64(len(destination)) > reader.remaining {
		return invalid("volume archive is truncated")
	}
	count, err := io.ReadFull(reader.source, destination)
	if err != nil || count != len(destination) {
		return invalid("volume archive is truncated")
	}
	_, _ = reader.digest.Write(destination)
	reader.remaining -= uint64(count)
	return nil
}

func (reader *archiveReader) zeroPadding(ctx context.Context, size uint64) error {
	padding := rounded(size) - size
	var buffer [TarBlockBytes]byte
	if err := reader.read(ctx, buffer[:int(padding)]); err != nil {
		return err
	}
	if !allZero(buffer[:int(padding)]) {
		return invalid("volume archive payload padding is nonzero")
	}
	return nil
}

func (reader *archiveReader) file(ctx context.Context, size uint64) ([sha256.Size]byte, error) {
	digest := sha256.New()
	buffer := make([]byte, 32*1024)
	remaining := size
	for remaining > 0 {
		want := uint64(len(buffer))
		if remaining < want {
			want = remaining
		}
		if err := reader.read(ctx, buffer[:int(want)]); err != nil {
			return [sha256.Size]byte{}, err
		}
		_, _ = digest.Write(buffer[:int(want)])
		remaining -= want
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func (reader *archiveReader) exactEOF() error {
	var extra [1]byte
	count, err := io.ReadFull(reader.source, extra[:])
	if count != 0 {
		return invalid("volume archive has trailing bytes")
	}
	if err != io.EOF {
		return invalid("volume archive exact-EOF check failed")
	}
	return nil
}
