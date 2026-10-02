package backupobject

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
)

// MetadataEvidence binds the entire required metadata set, including sizes and
// both digests. Length framing keeps arbitrary provider metadata values from
// producing ambiguous key/value concatenations.
func (artifact Artifact) MetadataEvidence() (uint32, [sha256.Size]byte) {
	return MetadataEvidence(artifact.Metadata())
}

func MetadataEvidence(metadata map[string]string) (uint32, [sha256.Size]byte) {
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	hash := sha256.New()
	_, _ = hash.Write([]byte("groundplane.backup.object-metadata.v1\x00"))
	var length [8]byte
	for _, key := range keys {
		binary.BigEndian.PutUint64(length[:], uint64(len(key)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(key))
		binary.BigEndian.PutUint64(length[:], uint64(len(metadata[key])))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(metadata[key]))
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return uint32(len(metadata)), digest
}
