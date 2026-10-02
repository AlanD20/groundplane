// Package backupvolumetransfer owns the schema-one wire representation of a
// managed Volume manifest. The archive package hashes these exact bytes.
package backupvolumetransfer

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

var deterministic = proto.MarshalOptions{Deterministic: true}

func EncodeEntries(entries []backupvolume.Entry) ([]backupvolume.ManifestEntryBytes, error) {
	if err := backupvolume.ValidateEntries(entries); err != nil {
		return nil, err
	}
	result := make([]backupvolume.ManifestEntryBytes, len(entries))
	for index, entry := range entries {
		wire := entryToWire(uint64(index+1), entry)
		encoded, err := deterministic.Marshal(wire)
		if err != nil || len(encoded) == 0 || len(encoded) > backupvolume.MaxManifestEntryBytes {
			return nil, invalidManifest()
		}
		result[index] = backupvolume.ManifestEntryBytes{Ordinal: uint64(index + 1), Bytes: encoded}
	}
	return result, nil
}

func DecodeEntries(
	wire []*agentpb.BackupVolumeManifestEntry,
) ([]backupvolume.Entry, []backupvolume.ManifestEntryBytes, error) {
	if len(wire) == 0 || len(wire) > backupvolume.MaxEntries {
		return nil, nil, invalidManifest()
	}
	entries := make([]backupvolume.Entry, len(wire))
	manifest := make([]backupvolume.ManifestEntryBytes, len(wire))
	for index, item := range wire {
		if item == nil || item.Ordinal != uint64(index+1) || item.ProtoReflect().GetUnknown() != nil ||
			len(item.ContentSha256) != sha256.Size {
			return nil, nil, invalidManifest()
		}
		entry, err := entryFromWire(item)
		if err != nil {
			return nil, nil, err
		}
		encoded, err := deterministic.Marshal(item)
		if err != nil || len(encoded) == 0 || len(encoded) > backupvolume.MaxManifestEntryBytes {
			return nil, nil, invalidManifest()
		}
		entries[index] = entry
		manifest[index] = backupvolume.ManifestEntryBytes{Ordinal: item.Ordinal, Bytes: encoded}
	}
	if err := backupvolume.ValidateEntries(entries); err != nil {
		return nil, nil, err
	}
	return entries, manifest, nil
}

func WireEntries(entries []backupvolume.Entry) ([]*agentpb.BackupVolumeManifestEntry, error) {
	if err := backupvolume.ValidateEntries(entries); err != nil {
		return nil, err
	}
	result := make([]*agentpb.BackupVolumeManifestEntry, len(entries))
	for index, entry := range entries {
		result[index] = entryToWire(uint64(index+1), entry)
	}
	return result, nil
}

func entryToWire(ordinal uint64, entry backupvolume.Entry) *agentpb.BackupVolumeManifestEntry {
	return &agentpb.BackupVolumeManifestEntry{Ordinal: ordinal,
		RelativePath: append([]byte(nil), entry.Path...), Kind: agentpb.BackupVolumeEntryKind(entry.Kind),
		Mode: entry.Mode, Uid: entry.UID, Gid: entry.GID, SizeBytes: entry.SizeBytes,
		ContentSha256: append([]byte(nil), entry.ContentSHA256[:]...),
	}
}

func entryFromWire(wire *agentpb.BackupVolumeManifestEntry) (backupvolume.Entry, error) {
	if wire.Kind != agentpb.BackupVolumeEntryKind_BACKUP_VOLUME_ENTRY_KIND_DIRECTORY &&
		wire.Kind != agentpb.BackupVolumeEntryKind_BACKUP_VOLUME_ENTRY_KIND_REGULAR_FILE {
		return backupvolume.Entry{}, invalidManifest()
	}
	entry := backupvolume.Entry{
		Path:      append([]byte(nil), wire.RelativePath...),
		Kind:      backupvolume.EntryKind(wire.Kind),
		Mode:      wire.Mode,
		UID:       wire.Uid,
		GID:       wire.Gid,
		SizeBytes: wire.SizeBytes,
	}
	copy(entry.ContentSHA256[:], wire.ContentSha256)
	return entry, nil
}

func ArchiveEvidenceToWire(value backupvolume.ArchiveEvidence) *agentpb.BackupVolumeArchiveEvidence {
	return &agentpb.BackupVolumeArchiveEvidence{EntryCount: value.EntryCount,
		ContentManifestSha256: append([]byte(nil), value.ContentManifestSHA256[:]...),
		FullTreeSha256:        append([]byte(nil), value.FullTreeSHA256[:]...), SourceSizeBytes: value.SourceSizeBytes}
}

func ArchiveEvidenceFromWire(wire *agentpb.BackupVolumeArchiveEvidence) (backupvolume.ArchiveEvidence, error) {
	if wire == nil || wire.EntryCount == 0 || wire.EntryCount > backupvolume.MaxEntries ||
		len(wire.ContentManifestSha256) != sha256.Size || len(wire.FullTreeSha256) != sha256.Size {
		return backupvolume.ArchiveEvidence{}, invalidManifest()
	}
	var value backupvolume.ArchiveEvidence
	value.EntryCount, value.SourceSizeBytes = wire.EntryCount, wire.SourceSizeBytes
	copy(value.ContentManifestSHA256[:], wire.ContentManifestSha256)
	copy(value.FullTreeSHA256[:], wire.FullTreeSha256)
	return value, nil
}

func VerifyEntries(entries []backupvolume.Entry, manifest []backupvolume.ManifestEntryBytes,
	expected *agentpb.BackupVolumeArchiveEvidence) error {
	value, err := ArchiveEvidenceFromWire(expected)
	if err != nil || len(entries) != int(value.EntryCount) {
		return invalidManifest()
	}
	content, err := backupvolume.ContentManifestSHA256(manifest)
	if err != nil || !bytes.Equal(content[:], value.ContentManifestSHA256[:]) {
		return invalidManifest()
	}
	tree, err := backupvolume.FullTreeSHA256(entries)
	if err != nil || !bytes.Equal(tree[:], value.FullTreeSHA256[:]) {
		return invalidManifest()
	}
	return nil
}

func invalidManifest() error {
	return errs.New(errs.KindValidationFailed, "Volume manifest differs from its sealed archive evidence")
}
