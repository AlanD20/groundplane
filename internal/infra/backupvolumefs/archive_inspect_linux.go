//go:build linux

package backupvolumefs

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"io"
	"math"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
)

// InspectCapturedArchive reconstructs manifest entries from a retained source
// artifact without reopening the live Volume. The first pass enumerates members
// and content; the canonical second pass binds every byte, entry, manifest,
// size, and digest to the prepared ArtifactEvidence. The caller must pin an
// immutable ReaderAt descriptor for both passes and for subsequent upload.
func InspectCapturedArchive(ctx context.Context, source io.ReaderAt,
	expected backupvolume.ArtifactEvidence,
	manifestFor func([]backupvolume.Entry) ([]backupvolume.ManifestEntryBytes, error),
) (backupvolume.ValidatedArchive, []backupvolume.ManifestEntryBytes, error) {
	if ctx == nil || source == nil || manifestFor == nil || expected.Source.SizeBytes > math.MaxInt64 {
		return backupvolume.ValidatedArchive{}, nil, invalid("retained Volume source input is invalid")
	}
	if err := ctx.Err(); err != nil {
		return backupvolume.ValidatedArchive{}, nil, err
	}
	if err := expected.Validate(); err != nil {
		return backupvolume.ValidatedArchive{}, nil, err
	}
	length := int64(expected.Source.SizeBytes)
	var extra [1]byte
	count, endErr := source.ReadAt(extra[:], length)
	if count != 0 || endErr != io.EOF {
		return backupvolume.ValidatedArchive{}, nil, invalid("retained Volume source has trailing bytes")
	}
	reader := tar.NewReader(io.NewSectionReader(source, 0, length))
	entries := make([]backupvolume.Entry, 0, expected.Archive.EntryCount)
	for {
		if err := ctx.Err(); err != nil {
			return backupvolume.ValidatedArchive{}, nil, err
		}
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil || len(entries) >= backupvolume.MaxEntries || header == nil {
			return backupvolume.ValidatedArchive{}, nil, invalid("retained Volume archive member is invalid")
		}
		entry, err := inspectedEntry(ctx, reader, header)
		if err != nil {
			return backupvolume.ValidatedArchive{}, nil, err
		}
		entries = append(entries, entry)
	}
	tree, err := TreeFromEntries(entries)
	if err != nil {
		return backupvolume.ValidatedArchive{}, nil, err
	}
	manifestInput := make([]backupvolume.Entry, len(tree.Entries))
	for i := range tree.Entries {
		manifestInput[i] = cloneEntry(tree.Entries[i])
	}
	manifest, err := manifestFor(manifestInput)
	if err != nil {
		return backupvolume.ValidatedArchive{}, nil, err
	}
	validated, err := backupvolume.Validate(ctx, io.NewSectionReader(source, 0, length), manifest, expected)
	if err != nil {
		return backupvolume.ValidatedArchive{}, nil, err
	}
	if len(validated.Entries) != len(tree.Entries) {
		return backupvolume.ValidatedArchive{}, nil, invalid("retained Volume archive entry count changed")
	}
	for i := range tree.Entries {
		if !sameEntry(validated.Entries[i], tree.Entries[i]) {
			return backupvolume.ValidatedArchive{}, nil, invalid("retained Volume archive entry changed")
		}
	}
	return validated, manifest, nil
}

func inspectedEntry(ctx context.Context, source io.Reader, header *tar.Header) (backupvolume.Entry, error) {
	if header.Name == "" || header.Mode < 0 || header.Mode > 0o7777 || header.Uid < 0 || header.Gid < 0 ||
		uint64(header.Uid) > math.MaxUint32 || uint64(header.Gid) > math.MaxUint32 || header.Size < 0 {
		return backupvolume.Entry{}, invalid("retained Volume archive entry metadata is invalid")
	}
	entry := backupvolume.Entry{Path: []byte(header.Name), Mode: uint32(header.Mode),
		UID: uint32(header.Uid), GID: uint32(header.Gid), SizeBytes: uint64(header.Size)}
	switch header.Typeflag {
	case tar.TypeDir:
		if entry.SizeBytes != 0 {
			return backupvolume.Entry{}, invalid("retained Volume directory has content")
		}
		entry.Kind = backupvolume.EntryDirectory
	case tar.TypeReg:
		entry.Kind = backupvolume.EntryRegular
		digest := sha256.New()
		remaining := entry.SizeBytes
		buffer := make([]byte, 32*1024)
		for remaining > 0 {
			if err := ctx.Err(); err != nil {
				return backupvolume.Entry{}, err
			}
			want := uint64(len(buffer))
			if want > remaining {
				want = remaining
			}
			count, err := io.ReadFull(source, buffer[:int(want)])
			if err != nil || count != int(want) {
				return backupvolume.Entry{}, invalid("retained Volume file is truncated")
			}
			_, _ = digest.Write(buffer[:count])
			remaining -= uint64(count)
		}
		copy(entry.ContentSHA256[:], digest.Sum(nil))
	default:
		return backupvolume.Entry{}, invalid("retained Volume archive contains an unsupported member")
	}
	return entry, nil
}
