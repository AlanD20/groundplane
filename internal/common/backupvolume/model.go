// Package backupvolume owns the canonical volume-tar-v1 artifact model,
// USTAR/PAX bytes, semantic digests, bounds, and strict reader.
package backupvolume

import (
	"bytes"
	"crypto/sha256"
	"math"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	Format = "volume-tar-v1"

	MaxEntries            = 2048
	MaxPathBytes          = 4095
	MaxManifestEntryBytes = 8192
	RestoreHeadroom       = 64 * 1024 * 1024
	TarBlockBytes         = 512
	maxPAXPayload         = MaxPathBytes + 128
	maximumMode           = 0o7777
	maximumSmallOctal     = 0o7777777
	maximumLargeOctal     = 0o77777777777
)

type EntryKind uint8

const (
	EntryDirectory EntryKind = 1
	EntryRegular   EntryKind = 2
)

// Entry ordinals are derived from slice position, starting at one.
type Entry struct {
	Path          []byte
	Kind          EntryKind
	Mode          uint32
	UID           uint32
	GID           uint32
	SizeBytes     uint64
	ContentSHA256 [sha256.Size]byte
}

// ManifestEntryBytes is one schema-owner-provided deterministic manifest
// entry. Bytes are hashed exactly as supplied; backupvolume does not interpret
// or encode their protocol fields.
type ManifestEntryBytes struct {
	Ordinal uint64
	Bytes   []byte
}

type ArchiveEvidence struct {
	EntryCount            uint64
	ContentManifestSHA256 [sha256.Size]byte
	FullTreeSHA256        [sha256.Size]byte
	SourceSizeBytes       uint64
}

type ArtifactEvidence struct {
	Source  backupformat.Evidence
	Archive ArchiveEvidence
}

type ValidatedArchive struct {
	Entries  []Entry
	Evidence ArtifactEvidence
}

func (evidence ArtifactEvidence) Validate() error {
	if err := evidence.Source.Validate(backupformat.MaxStoredBytes); err != nil {
		return err
	}
	if evidence.Archive.EntryCount < 1 || evidence.Archive.EntryCount > MaxEntries {
		return invalid("volume archive entry count is invalid")
	}
	if evidence.Archive.SourceSizeBytes != evidence.Source.SizeBytes {
		return invalid("volume archive source sizes do not match")
	}
	if evidence.Source.SizeBytes < 3*TarBlockBytes || evidence.Source.SizeBytes%TarBlockBytes != 0 {
		return invalid("volume archive source size is not canonical")
	}
	return nil
}

func ValidateEntries(entries []Entry) error {
	if len(entries) < 1 || len(entries) > MaxEntries {
		return invalid("volume archive must contain 1 through 2048 entries")
	}
	for index := range entries {
		if err := validateEntryShape(entries[index], index); err != nil {
			return err
		}
		if index > 1 && bytes.Compare(entries[index-1].Path, entries[index].Path) >= 0 {
			return invalid("volume archive paths must be unique and byte-sorted")
		}
		if _, err := planHeader(entries[index], uint64(index)); err != nil {
			return err
		}
	}
	return nil
}

// ValidateManifest verifies the concrete deterministic-byte sequence supplied
// by the manifest schema owner.
func ValidateManifest(manifest []ManifestEntryBytes, expectedCount int) error {
	if expectedCount < 1 || expectedCount > MaxEntries || len(manifest) != expectedCount {
		return invalid("volume manifest entry count does not match")
	}
	for index, entry := range manifest {
		if entry.Ordinal != uint64(index+1) {
			return invalid("volume manifest ordinals are not canonical")
		}
		if len(entry.Bytes) < 1 || len(entry.Bytes) > MaxManifestEntryBytes {
			return invalid("volume manifest entry byte length is invalid")
		}
	}
	return nil
}

func validateEntryShape(entry Entry, index int) error {
	if len(entry.Path) < 1 || len(entry.Path) > MaxPathBytes {
		return invalid("volume archive path length is invalid")
	}
	if bytes.IndexByte(entry.Path, 0) >= 0 {
		return invalid("volume archive path contains NUL")
	}
	if index == 0 {
		if !bytes.Equal(entry.Path, []byte(".")) || entry.Kind != EntryDirectory {
			return invalid("volume archive root must be the directory dot entry")
		}
	} else {
		if bytes.Equal(entry.Path, []byte(".")) || entry.Path[0] == '/' || entry.Path[len(entry.Path)-1] == '/' {
			return invalid("volume archive descendant path is not relative and canonical")
		}
		for _, component := range bytes.Split(entry.Path, []byte{'/'}) {
			if len(component) == 0 || bytes.Equal(component, []byte(".")) || bytes.Equal(component, []byte("..")) {
				return invalid("volume archive path contains an invalid component")
			}
		}
	}
	if entry.Mode > maximumMode {
		return invalid("volume archive mode contains bits above the low twelve")
	}
	switch entry.Kind {
	case EntryDirectory:
		if entry.SizeBytes != 0 || entry.ContentSHA256 != ([sha256.Size]byte{}) {
			return invalid("volume archive directory must have zero size and digest")
		}
	case EntryRegular:
		if entry.SizeBytes > backupformat.MaxStoredBytes {
			return invalid("volume archive file exceeds the source limit")
		}
	default:
		return invalid("volume archive entry kind is invalid")
	}
	return nil
}

func archiveSize(entries []Entry) (uint64, error) {
	if err := ValidateEntries(entries); err != nil {
		return 0, err
	}
	size := uint64(2 * TarBlockBytes)
	for index, entry := range entries {
		plan, err := planHeader(entry, uint64(index))
		if err != nil {
			return 0, err
		}
		addition := uint64(TarBlockBytes) + rounded(entry.SizeBytes)
		if plan.hasPAX {
			addition += uint64(TarBlockBytes) + rounded(uint64(len(plan.paxPayload)))
		}
		if addition > math.MaxUint64-size {
			return 0, invalid("volume archive size arithmetic overflowed")
		}
		size += addition
		if size > backupformat.MaxStoredBytes {
			return 0, invalid("volume archive exceeds the source limit")
		}
	}
	return size, nil
}

func rounded(value uint64) uint64 {
	if value%TarBlockBytes == 0 {
		return value
	}
	return value + TarBlockBytes - value%TarBlockBytes
}

func cloneEntries(entries []Entry) []Entry {
	result := make([]Entry, len(entries))
	for index := range entries {
		result[index] = entries[index]
		result[index].Path = append([]byte(nil), entries[index].Path...)
	}
	return result
}

func invalid(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}

func internal(message string) error {
	return errs.New(errs.KindInternal, message)
}
