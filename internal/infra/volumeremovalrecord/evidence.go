package volumeremovalrecord

import (
	"crypto/sha256"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const EvidenceRecordBytes = 16 * 1024

// EvidenceRow is one accepted consumer/mount intent, not desired state or
// permission to run a filesystem helper. Ordinals are dense and one-based.
type EvidenceRow struct {
	OperationID      string
	VolumeID         string
	ConsumerID       string
	SourceRevisionID string
	Ordinal          uint64
	MountTarget      string
	ReadOnly         bool
	SHA256           [sha256.Size]byte
}

// EvidenceManifest pins the fixed-revision accepted set and the single desired
// revision that may publish it. Row order is committed by the rolling digest.
type EvidenceManifest struct {
	OperationID       string
	EnvironmentID     string
	VolumeID          string
	Key               string
	ReadRevision      int64
	SourceRevisionID  string
	DesiredRevisionID string
	ImpactSHA256      [sha256.Size]byte
	TotalRows         uint64
	MaximumOrdinal    uint64
	OrderedSHA256     [sha256.Size]byte
}

// EvidenceCursor records bounded prefix progress. It does not, by itself,
// prove that rows exist in storage or authorize desired publication.
type EvidenceCursor struct {
	OperationID    string
	NextOrdinal    uint64
	CompletedRows  uint64
	LastConsumerID string
	RollingSHA256  [sha256.Size]byte
	ManifestSHA256 [sha256.Size]byte
}

func EmptyEvidenceDigest() [sha256.Size]byte {
	return sha256.Sum256([]byte("groundplane-volume-removal-evidence-v1"))
}

func AppendEvidenceDigest(previous, row [sha256.Size]byte) [sha256.Size]byte {
	var value [2 * sha256.Size]byte
	copy(value[:sha256.Size], previous[:])
	copy(value[sha256.Size:], row[:])
	return sha256.Sum256(value[:])
}

func EvidenceRowDigest(row EvidenceRow) ([sha256.Size]byte, error) {
	value, err := encodeEvidenceRowBody(row)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	defer clear(value)
	return sha256.Sum256(value), nil
}

func EncodeEvidenceRow(row EvidenceRow) ([]byte, error) {
	value, err := encodeEvidenceRowBody(row)
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(value) != row.SHA256 {
		clear(value)
		return nil, invalidEvidence()
	}
	return append(value, row.SHA256[:]...), nil
}

func encodeEvidenceRowBody(row EvidenceRow) ([]byte, error) {
	if ids.Validate(ids.KindOperation, row.OperationID) != nil ||
		ids.Validate(ids.KindVolume, row.VolumeID) != nil || ids.Validate(ids.KindService, row.ConsumerID) != nil ||
		ids.Validate(ids.KindTask, row.SourceRevisionID) != nil || row.Ordinal == 0 || row.Ordinal == math.MaxUint64 ||
		!strings.HasPrefix(row.MountTarget, "/") || strings.ContainsRune(row.MountTarget, 0) ||
		!utf8.ValidString(row.MountTarget) || len(row.MountTarget) > EvidenceRecordBytes {
		return nil, invalidEvidence()
	}
	writer := volumeRemovalWriter{value: []byte("GVER")}
	writer.uint16(1)
	writer.string(row.OperationID)
	writer.string(row.VolumeID)
	writer.string(row.ConsumerID)
	writer.string(row.SourceRevisionID)
	writer.uint64(row.Ordinal)
	writer.string(row.MountTarget)
	writer.boolean(row.ReadOnly)
	if len(writer.value)+sha256.Size > EvidenceRecordBytes {
		clear(writer.value)
		return nil, invalidEvidence()
	}
	return finishVolumeRemovalEncoding(writer)
}

func DecodeEvidenceRow(value []byte) (EvidenceRow, error) {
	reader, err := evidenceReader(value, "GVER")
	if err != nil {
		return EvidenceRow{}, err
	}
	row := EvidenceRow{OperationID: reader.string(128), VolumeID: reader.string(128), ConsumerID: reader.string(128),
		SourceRevisionID: reader.string(128), Ordinal: reader.uint64(), MountTarget: reader.string(EvidenceRecordBytes),
		ReadOnly: reader.boolean(), SHA256: reader.digest()}
	digest, err := EvidenceRowDigest(row)
	if reader.done() != nil || err != nil || digest != row.SHA256 {
		return EvidenceRow{}, Corrupt()
	}
	return row, nil
}

func EncodeEvidenceManifest(manifest EvidenceManifest) ([]byte, error) {
	if ids.Validate(ids.KindOperation, manifest.OperationID) != nil ||
		ids.Validate(ids.KindEnvironment, manifest.EnvironmentID) != nil ||
		ids.Validate(ids.KindVolume, manifest.VolumeID) != nil || volumeidentity.ValidateKey(manifest.Key) != nil ||
		manifest.ReadRevision <= 0 || ids.Validate(ids.KindTask, manifest.SourceRevisionID) != nil ||
		ids.Validate(ids.KindTask, manifest.DesiredRevisionID) != nil ||
		manifest.SourceRevisionID == manifest.DesiredRevisionID || zeroVolumeRemovalDigest(manifest.ImpactSHA256) ||
		manifest.TotalRows == math.MaxUint64 || manifest.TotalRows != manifest.MaximumOrdinal ||
		zeroVolumeRemovalDigest(manifest.OrderedSHA256) ||
		(manifest.TotalRows == 0 && manifest.OrderedSHA256 != EmptyEvidenceDigest()) {
		return nil, invalidEvidence()
	}
	writer := volumeRemovalWriter{value: []byte("GVEM")}
	writer.uint16(1)
	writer.string(manifest.OperationID)
	writer.string(manifest.EnvironmentID)
	writer.string(manifest.VolumeID)
	writer.string(manifest.Key)
	writer.uint64(uint64(manifest.ReadRevision))
	writer.string(manifest.SourceRevisionID)
	writer.string(manifest.DesiredRevisionID)
	writer.digest(manifest.ImpactSHA256)
	writer.uint64(manifest.TotalRows)
	writer.uint64(manifest.MaximumOrdinal)
	writer.digest(manifest.OrderedSHA256)
	return finishEvidenceEncoding(writer)
}

func DecodeEvidenceManifest(value []byte) (EvidenceManifest, error) {
	reader, err := evidenceReader(value, "GVEM")
	if err != nil {
		return EvidenceManifest{}, err
	}
	manifest := EvidenceManifest{OperationID: reader.string(128), EnvironmentID: reader.string(128),
		VolumeID: reader.string(128), Key: reader.string(255)}
	revision := reader.uint64()
	if revision > math.MaxInt64 {
		return EvidenceManifest{}, Corrupt()
	}
	manifest.ReadRevision = int64(revision)
	manifest.SourceRevisionID, manifest.DesiredRevisionID = reader.string(128), reader.string(128)
	manifest.ImpactSHA256 = reader.digest()
	manifest.TotalRows, manifest.MaximumOrdinal = reader.uint64(), reader.uint64()
	manifest.OrderedSHA256 = reader.digest()
	encoded, err := EncodeEvidenceManifest(manifest)
	clear(encoded)
	if reader.done() != nil || err != nil {
		return EvidenceManifest{}, Corrupt()
	}
	return manifest, nil
}

func InitialEvidenceCursor(manifest EvidenceManifest) (EvidenceCursor, error) {
	value, err := EncodeEvidenceManifest(manifest)
	if err != nil {
		return EvidenceCursor{}, err
	}
	defer clear(value)
	return EvidenceCursor{OperationID: manifest.OperationID, NextOrdinal: 1,
		RollingSHA256: EmptyEvidenceDigest(), ManifestSHA256: sha256.Sum256(value)}, nil
}

func AdvanceEvidenceCursor(cursor EvidenceCursor, manifest EvidenceManifest, row EvidenceRow) (EvidenceCursor, error) {
	if validateEvidenceCursor(cursor, manifest) != nil || cursor.CompletedRows >= manifest.TotalRows ||
		row.Ordinal != cursor.NextOrdinal || row.OperationID != manifest.OperationID || row.VolumeID != manifest.VolumeID ||
		row.SourceRevisionID != manifest.SourceRevisionID || row.ConsumerID < cursor.LastConsumerID {
		return EvidenceCursor{}, invalidEvidence()
	}
	digest, err := EvidenceRowDigest(row)
	if err != nil || digest != row.SHA256 {
		return EvidenceCursor{}, invalidEvidence()
	}
	cursor.CompletedRows++
	cursor.NextOrdinal++
	cursor.LastConsumerID = row.ConsumerID
	cursor.RollingSHA256 = AppendEvidenceDigest(cursor.RollingSHA256, row.SHA256)
	if err := validateEvidenceCursor(cursor, manifest); err != nil {
		return EvidenceCursor{}, err
	}
	return cursor, nil
}

func validateEvidenceCursor(cursor EvidenceCursor, manifest EvidenceManifest) error {
	initial, err := InitialEvidenceCursor(manifest)
	if err != nil || cursor.OperationID != manifest.OperationID || cursor.ManifestSHA256 != initial.ManifestSHA256 ||
		cursor.CompletedRows > manifest.TotalRows || cursor.NextOrdinal != cursor.CompletedRows+1 ||
		(cursor.CompletedRows == 0 && cursor.LastConsumerID != "") ||
		(cursor.CompletedRows > 0 && ids.Validate(ids.KindService, cursor.LastConsumerID) != nil) ||
		zeroVolumeRemovalDigest(cursor.RollingSHA256) ||
		(cursor.CompletedRows == 0 && cursor.RollingSHA256 != initial.RollingSHA256) ||
		(cursor.CompletedRows == manifest.TotalRows && cursor.RollingSHA256 != manifest.OrderedSHA256) {
		return invalidEvidence()
	}
	return nil
}

func EncodeEvidenceCursor(cursor EvidenceCursor, manifest EvidenceManifest) ([]byte, error) {
	if err := validateEvidenceCursor(cursor, manifest); err != nil {
		return nil, err
	}
	writer := volumeRemovalWriter{value: []byte("GVEC")}
	writer.uint16(1)
	writer.string(cursor.OperationID)
	writer.uint64(cursor.NextOrdinal)
	writer.uint64(cursor.CompletedRows)
	writer.string(cursor.LastConsumerID)
	writer.digest(cursor.RollingSHA256)
	writer.digest(cursor.ManifestSHA256)
	return finishEvidenceEncoding(writer)
}

func DecodeEvidenceCursor(value []byte, manifest EvidenceManifest) (EvidenceCursor, error) {
	reader, err := evidenceReader(value, "GVEC")
	if err != nil {
		return EvidenceCursor{}, err
	}
	cursor := EvidenceCursor{
		OperationID:    reader.string(128),
		NextOrdinal:    reader.uint64(),
		CompletedRows:  reader.uint64(),
		LastConsumerID: reader.string(128),
		RollingSHA256:  reader.digest(),
		ManifestSHA256: reader.digest(),
	}
	if reader.done() != nil || validateEvidenceCursor(cursor, manifest) != nil {
		return EvidenceCursor{}, Corrupt()
	}
	return cursor, nil
}

func evidenceReader(value []byte, magic string) (volumeRemovalReader, error) {
	if len(value) > EvidenceRecordBytes {
		return volumeRemovalReader{}, Corrupt()
	}
	return newVolumeRemovalReader(value, magic)
}

func finishEvidenceEncoding(writer volumeRemovalWriter) ([]byte, error) {
	if len(writer.value) > EvidenceRecordBytes {
		clear(writer.value)
		return nil, invalidEvidence()
	}
	return finishVolumeRemovalEncoding(writer)
}

func invalidEvidence() error {
	return errs.New(errs.KindValidationFailed, "volume removal evidence is invalid")
}
