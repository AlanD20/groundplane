//go:build linux

package backupvolumefs

import (
	"archive/tar"
	"context"
	"io"
	"math"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
)

// ConstructArchive revalidates the complete canonical source from the retained
// ReaderAt before changing the hidden sibling. The caller must pin this staged
// source descriptor and prevent writes to it until construction and exchange
// finish. startOrdinal is the next archive ordinal from the durable journal;
// pending applies only to that first construction. The root is constructed by
// CreateReplacement, so startOrdinal must be at least two.
func (replacement *Replacement) ConstructArchive(ctx context.Context, source io.ReaderAt,
	validated backupvolume.ValidatedArchive, manifest []backupvolume.ManifestEntryBytes,
	startOrdinal uint64, pending bool, journal Journal,
) error {
	if err := replacement.ready(ctx, journal); err != nil {
		return err
	}
	if source == nil || startOrdinal < 2 || startOrdinal > uint64(len(validated.Entries))+1 ||
		validated.Evidence.Source.SizeBytes > math.MaxInt64 {
		return invalid("replacement Volume archive construction input is invalid")
	}
	length := int64(validated.Evidence.Source.SizeBytes)
	var extra [1]byte
	count, endErr := source.ReadAt(extra[:], length)
	if count != 0 || endErr != io.EOF {
		return invalid("replacement Volume source has trailing bytes")
	}
	proved, err := backupvolume.Validate(ctx, io.NewSectionReader(source, 0, length), manifest, validated.Evidence)
	if err != nil {
		return err
	}
	if len(proved.Entries) != len(validated.Entries) {
		return invalid("replacement Volume archive entry count changed")
	}
	for index := range proved.Entries {
		if !sameEntry(proved.Entries[index], validated.Entries[index]) {
			return invalid("replacement Volume archive entry changed")
		}
	}
	reader := tar.NewReader(io.NewSectionReader(source, 0, length))
	for index, entry := range proved.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := reader.Next()
		if err != nil {
			return invalid("replacement Volume archive member is missing")
		}
		if !matchesArchiveHeader(header, entry) {
			return invalid("replacement Volume archive member changed")
		}
		ordinal := uint64(index + 1)
		if ordinal < startOrdinal {
			continue
		}
		if ordinal == 1 {
			return invalid("replacement Volume archive root construction is separate")
		}
		var content io.Reader
		if entry.Kind == backupvolume.EntryRegular {
			content = reader
		}
		if err := replacement.Construct(
			ctx,
			ordinal,
			entry,
			content,
			pending && ordinal == startOrdinal,
			journal,
		); err != nil {
			return err
		}
	}
	if _, err := reader.Next(); err != io.EOF {
		return invalid("replacement Volume archive has an unexpected member")
	}
	return nil
}

func matchesArchiveHeader(header *tar.Header, entry backupvolume.Entry) bool {
	if header == nil || header.Name != string(entry.Path) || header.Mode != int64(entry.Mode) ||
		header.Uid != int(entry.UID) || header.Gid != int(entry.GID) ||
		header.Size != int64(entry.SizeBytes) {
		return false
	}
	if entry.Kind == backupvolume.EntryDirectory {
		return header.Typeflag == tar.TypeDir
	}
	return entry.Kind == backupvolume.EntryRegular && header.Typeflag == tar.TypeReg
}
