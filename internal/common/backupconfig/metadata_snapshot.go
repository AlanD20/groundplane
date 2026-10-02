package backupconfig

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
)

// MetadataSnapshotSHA256 authenticates the canonical protocol metadata, not
// selected plaintext. The protocol encoder owns unknown-field-free protobuf
// encoding; this owner checks the canonical Entry order and exact framing.
func MetadataSnapshotSHA256(
	ctx context.Context,
	entries []Entry,
	frames []MetadataFrame,
) ([32]byte, uint64, error) {
	if err := checkContext(ctx); err != nil {
		return [32]byte{}, 0, err
	}
	if err := validateEntries(entries); err != nil {
		return [32]byte{}, 0, err
	}
	if err := validateMetadataFrames(entries, frames); err != nil {
		return [32]byte{}, 0, err
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("groundplane.backup.config.metadata-snapshot.v1\x00"))
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(len(frames)))
	_, _ = hasher.Write(number[:])
	var total uint64
	for _, frame := range frames {
		if err := checkContext(ctx); err != nil {
			return [32]byte{}, 0, err
		}
		binary.BigEndian.PutUint64(number[:], uint64(frame.Ordinal))
		_, _ = hasher.Write(number[:])
		binary.BigEndian.PutUint64(number[:], uint64(len(frame.CanonicalEntry)))
		_, _ = hasher.Write(number[:])
		_, _ = hasher.Write(frame.CanonicalEntry)
		total += uint64(len(frame.CanonicalEntry))
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, total, nil
}
