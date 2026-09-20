package runtimeconfiguration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	schemaVersion             = 1
	maximumEncodedRecordBytes = 256 << 10
	maximumReferenceBytes     = 1024
)

type manifestRecord struct {
	Schema        uint32           `json:"schema"`
	ID            string           `json:"id"`
	EnvironmentID string           `json:"environment_id"`
	Generation    uint64           `json:"generation"`
	Members       []manifestMember `json:"members"`
}

type manifestMember struct {
	Destination   string `json:"destination"`
	ContentSHA256 string `json:"content_sha256"`
	RecordSHA256  string `json:"record_sha256"`
}

type memberRecord struct {
	Schema        uint32                     `json:"schema"`
	SnapshotID    string                     `json:"snapshot_id"`
	EnvironmentID string                     `json:"environment_id"`
	Generation    uint64                     `json:"generation"`
	Record        taskmaterialization.Record `json:"record"`
}

type canonicalSnapshot struct {
	snapshot  Snapshot
	manifest  manifestRecord
	rootValue []byte
	reference Reference
	members   []canonicalMember
}

type canonicalMember struct {
	index manifestMember
	key   string
	value []byte
}

// EncodeReference is the sole persisted representation of a retained
// configuration-source reference.
func EncodeReference(reference Reference) ([]byte, error) {
	if err := ValidateReference(reference); err != nil {
		return nil, err
	}
	value, err := json.Marshal(reference)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(value) > maximumReferenceBytes {
		return nil, errs.New(errs.KindValidationFailed, "runtime configuration reference is too large")
	}
	return value, nil
}

// DecodeReference rejects non-canonical, unknown, duplicate, and trailing
// persisted fields rather than creating a second reference format.
func DecodeReference(value []byte) (Reference, error) {
	if len(value) == 0 || len(value) > maximumReferenceBytes {
		return Reference{}, corrupt("runtime configuration reference is corrupt")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var reference Reference
	if decoder.Decode(&reference) != nil || requireEOF(decoder) != nil || ValidateReference(reference) != nil {
		return Reference{}, corrupt("runtime configuration reference is corrupt")
	}
	canonical, err := json.Marshal(reference)
	if err != nil || !bytes.Equal(canonical, value) {
		return Reference{}, corrupt("runtime configuration reference is not canonical")
	}
	return reference, nil
}

func canonicalizeSnapshot(snapshot Snapshot) (canonicalSnapshot, error) {
	canonical, err := validateAndSortSnapshot(snapshot)
	if err != nil {
		return canonicalSnapshot{}, err
	}
	result := canonicalSnapshot{
		snapshot: canonical,
		manifest: manifestRecord{
			Schema: schemaVersion, ID: canonical.ID, EnvironmentID: canonical.EnvironmentID,
			Generation: canonical.Generation, Members: make([]manifestMember, 0, len(canonical.Files)),
		},
		members: make([]canonicalMember, 0, len(canonical.Files)),
	}
	for _, record := range canonical.Files {
		stored := memberRecord{
			Schema: schemaVersion, SnapshotID: canonical.ID, EnvironmentID: canonical.EnvironmentID,
			Generation: canonical.Generation, Record: record,
		}
		value, encodeErr := encodeMember(stored)
		if encodeErr != nil {
			return canonicalSnapshot{}, encodeErr
		}
		index := manifestMember{
			Destination: record.Destination, ContentSHA256: record.SHA256, RecordSHA256: digest(value),
		}
		result.manifest.Members = append(result.manifest.Members, index)
		result.members = append(result.members, canonicalMember{
			index: index, key: memberKey(canonical.ID, index), value: value,
		})
	}
	result.rootValue, err = encodeManifest(result.manifest)
	if err != nil {
		return canonicalSnapshot{}, err
	}
	result.reference = Reference{
		ID: canonical.ID, EnvironmentID: canonical.EnvironmentID,
		Generation: canonical.Generation, SHA256: digest(result.rootValue),
	}
	return result, nil
}

func encodeManifest(manifest manifestRecord) ([]byte, error) {
	if validateManifest(manifest) != nil {
		return nil, errs.New(errs.KindValidationFailed, "runtime configuration manifest is invalid")
	}
	value, err := json.Marshal(manifest)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(value) > maximumEncodedRecordBytes {
		return nil, errs.New(errs.KindValidationFailed, "runtime configuration manifest is too large")
	}
	return value, nil
}

func decodeManifest(value []byte) (manifestRecord, error) {
	if len(value) == 0 || len(value) > maximumEncodedRecordBytes {
		return manifestRecord{}, corrupt("runtime configuration manifest is corrupt")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var manifest manifestRecord
	if decoder.Decode(&manifest) != nil || requireEOF(decoder) != nil || validateManifest(manifest) != nil {
		return manifestRecord{}, corrupt("runtime configuration manifest is corrupt")
	}
	canonical, err := json.Marshal(manifest)
	if err != nil || !bytes.Equal(canonical, value) {
		return manifestRecord{}, corrupt("runtime configuration manifest is not canonical")
	}
	return manifest, nil
}

func validateManifest(manifest manifestRecord) error {
	if manifest.Schema != schemaVersion || ids.Validate(ids.KindConfig, manifest.ID) != nil ||
		ids.Validate(ids.KindEnvironment, manifest.EnvironmentID) != nil || manifest.Generation == 0 ||
		len(manifest.Members) > MaximumFiles || manifest.Members == nil {
		return corrupt("runtime configuration manifest fields are invalid")
	}
	previousDestination := ""
	for _, member := range manifest.Members {
		if entrymaterialization.ValidateDesiredDestination(member.Destination) != nil ||
			!validSHA256(member.ContentSHA256) || !validSHA256(member.RecordSHA256) ||
			(previousDestination != "" && member.Destination <= previousDestination) {
			return corrupt("runtime configuration manifest member is invalid")
		}
		previousDestination = member.Destination
	}
	return nil
}

func encodeMember(record memberRecord) ([]byte, error) {
	if validateMemberRecord(record) != nil {
		return nil, errs.New(errs.KindValidationFailed, "runtime configuration member is invalid")
	}
	value, err := json.Marshal(record)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(value) > maximumEncodedRecordBytes {
		return nil, errs.New(errs.KindValidationFailed, "runtime configuration member is too large")
	}
	return value, nil
}

func decodeMember(value []byte) (memberRecord, error) {
	if len(value) == 0 || len(value) > maximumEncodedRecordBytes {
		return memberRecord{}, corrupt("runtime configuration member is corrupt")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var record memberRecord
	if decoder.Decode(&record) != nil || requireEOF(decoder) != nil || validateMemberRecord(record) != nil {
		return memberRecord{}, corrupt("runtime configuration member is corrupt")
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(canonical, value) {
		return memberRecord{}, corrupt("runtime configuration member is not canonical")
	}
	return record, nil
}

func validateMemberRecord(record memberRecord) error {
	if record.Schema != schemaVersion || ids.Validate(ids.KindConfig, record.SnapshotID) != nil ||
		ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil || record.Generation == 0 ||
		record.Record.EnvironmentID != record.EnvironmentID ||
		taskmaterialization.ValidateRecord(record.Record, record.Generation) != nil {
		return corrupt("runtime configuration member fields are invalid")
	}
	return nil
}

func requireEOF(decoder *json.Decoder) error {
	var trailing struct{}
	if err := decoder.Decode(&trailing); err != io.EOF {
		return corrupt("runtime configuration record has trailing data")
	}
	return nil
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func corrupt(message string) error { return errs.New(errs.KindInternal, message) }
