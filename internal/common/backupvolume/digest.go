package backupvolume

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

const (
	volumeTreeDomain     = "groundplane.volume-tree.v1"
	volumeManifestDomain = "groundplane.backup.volume-manifest-content.v1"
)

// ContentManifestSHA256 hashes the exact deterministic manifest-entry bytes
// supplied by the schema owner without interpreting or re-encoding them.
func ContentManifestSHA256(manifest []ManifestEntryBytes) ([sha256.Size]byte, error) {
	if err := ValidateManifest(manifest, len(manifest)); err != nil {
		return [sha256.Size]byte{}, err
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte(volumeManifestDomain))
	_, _ = digest.Write([]byte{0})
	writeUint32(digest, uint32(len(manifest)))
	for _, entry := range manifest {
		writeUint32(digest, uint32(len(entry.Bytes)))
		_, _ = digest.Write(entry.Bytes)
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func FullTreeSHA256(entries []Entry) ([sha256.Size]byte, error) {
	if err := ValidateEntries(entries); err != nil {
		return [sha256.Size]byte{}, err
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte(volumeTreeDomain))
	_, _ = digest.Write([]byte{0})
	writeUint64(digest, uint64(len(entries)))
	for _, entry := range entries {
		writeUint32(digest, uint32(len(entry.Path)))
		_, _ = digest.Write(entry.Path)
		_, _ = digest.Write([]byte{byte(entry.Kind)})
		writeUint32(digest, entry.Mode)
		writeUint32(digest, entry.UID)
		writeUint32(digest, entry.GID)
		writeUint64(digest, entry.SizeBytes)
		_, _ = digest.Write(entry.ContentSHA256[:])
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func writeUint32(destination hash.Hash, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = destination.Write(encoded[:])
}

func writeUint64(destination hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = destination.Write(encoded[:])
}
