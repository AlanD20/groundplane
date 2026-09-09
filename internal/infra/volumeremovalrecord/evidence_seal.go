package volumeremovalrecord

import (
	"crypto/sha256"
	"math"
)

func EvidenceSealKey(operationID string) string { return EvidenceRoot(operationID) + "seal" }

// EvidenceSeal witnesses a full fixed-revision scan of the immutable row set.
// Publication must fence the seal and these exact manifest/cursor revisions;
// the value alone is not authorization to publish or execute a removal.
type EvidenceSeal struct {
	ManifestSHA256   [sha256.Size]byte
	ManifestRevision int64
	CursorRevision   int64
	VerifiedRevision int64
}

func EncodeEvidenceSeal(seal EvidenceSeal, manifest EvidenceManifest) ([]byte, error) {
	value, err := EncodeEvidenceManifest(manifest)
	if err != nil {
		return nil, err
	}
	defer clear(value)
	if seal.ManifestSHA256 != sha256.Sum256(value) || seal.ManifestRevision < manifest.ReadRevision ||
		seal.CursorRevision < seal.ManifestRevision || seal.VerifiedRevision < seal.CursorRevision {
		return nil, invalidEvidence()
	}
	writer := volumeRemovalWriter{value: []byte("GVES")}
	writer.uint16(1)
	writer.digest(seal.ManifestSHA256)
	writer.uint64(uint64(seal.ManifestRevision))
	writer.uint64(uint64(seal.CursorRevision))
	writer.uint64(uint64(seal.VerifiedRevision))
	return finishEvidenceEncoding(writer)
}

func DecodeEvidenceSeal(value []byte, manifest EvidenceManifest) (EvidenceSeal, error) {
	reader, err := evidenceReader(value, "GVES")
	if err != nil {
		return EvidenceSeal{}, err
	}
	seal := EvidenceSeal{ManifestSHA256: reader.digest()}
	manifestRevision, cursorRevision, verifiedRevision := reader.uint64(), reader.uint64(), reader.uint64()
	if manifestRevision > math.MaxInt64 || cursorRevision > math.MaxInt64 || verifiedRevision > math.MaxInt64 {
		return EvidenceSeal{}, Corrupt()
	}
	seal.ManifestRevision, seal.CursorRevision, seal.VerifiedRevision = int64(
		manifestRevision,
	), int64(
		cursorRevision,
	), int64(
		verifiedRevision,
	)
	encoded, err := EncodeEvidenceSeal(seal, manifest)
	clear(encoded)
	if reader.done() != nil || err != nil {
		return EvidenceSeal{}, Corrupt()
	}
	return seal, nil
}
