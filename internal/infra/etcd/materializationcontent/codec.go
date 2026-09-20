package materializationcontent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	schemaVersion             = 1
	maximumEncodedRecordBytes = 256 << 10
	chunkContentBytes         = int(entrymaterialization.MaximumChunkBytes)
)

type manifestRecord struct {
	Schema     uint32                     `json:"schema"`
	Generation uint64                     `json:"generation"`
	Record     taskmaterialization.Record `json:"record"`
	Chunks     []string                   `json:"chunks"`
}

type chunkRecord struct {
	Schema            uint32 `json:"schema"`
	EnvironmentID     string `json:"environment_id"`
	MaterializationID string `json:"materialization_id"`
	Ordinal           uint32 `json:"ordinal"`
	Offset            uint64 `json:"offset"`
	SHA256            string `json:"sha256"`
	Content           []byte `json:"content"`
}

type canonicalContent struct {
	manifestValue   []byte
	manifest        manifestRecord
	chunks          []chunkRecord
	chunkValues     [][]byte
	publishedRoot   string
	stagingRoot     string
	materialization taskmaterialization.Record
}

func canonicalize(
	record taskmaterialization.Record,
	generation uint64,
	content []byte,
) (canonicalContent, error) {
	if err := validateComponentRecord(record, generation); err != nil {
		return canonicalContent{}, err
	}
	if uint64(len(content)) != record.Length || digest(content) != record.SHA256 {
		return canonicalContent{}, errs.New(
			errs.KindValidationFailed,
			"component materialization content is inconsistent",
		)
	}
	result := canonicalContent{
		manifest: manifestRecord{
			Schema: schemaVersion, Generation: generation,
			Record: taskmaterialization.Clone([]taskmaterialization.Record{record})[0], Chunks: []string{},
		},
		materialization: record,
		publishedRoot:   publishedRootKey(record.EnvironmentID, record.MaterializationID),
		stagingRoot:     stagingRootKey(record.EnvironmentID, record.MaterializationID),
	}
	for start, ordinal := 0, uint32(0); start < len(content); start, ordinal = start+chunkContentBytes, ordinal+1 {
		end := min(start+chunkContentBytes, len(content))
		chunk := chunkRecord{
			Schema: schemaVersion, EnvironmentID: record.EnvironmentID,
			MaterializationID: record.MaterializationID, Ordinal: ordinal, Offset: uint64(start),
			SHA256: digest(content[start:end]), Content: append([]byte(nil), content[start:end]...),
		}
		value, err := encodeChunk(chunk)
		if err != nil {
			return canonicalContent{}, err
		}
		result.chunks = append(result.chunks, chunk)
		result.chunkValues = append(result.chunkValues, value)
		result.manifest.Chunks = append(result.manifest.Chunks, digest(value))
	}
	value, err := encodeManifest(result.manifest)
	if err != nil {
		return canonicalContent{}, err
	}
	result.manifestValue = value
	return result, nil
}

func encodeManifest(record manifestRecord) ([]byte, error) {
	if validateManifest(record) != nil {
		return nil, errs.New(errs.KindValidationFailed, "component materialization manifest is invalid")
	}
	return encodeCanonical(record, "component materialization manifest")
}

func decodeManifest(value []byte) (manifestRecord, error) {
	record, err := decodeCanonical[manifestRecord](value, "component materialization manifest")
	if err != nil || validateManifest(record) != nil {
		return manifestRecord{}, corrupt("component materialization manifest is corrupt")
	}
	return record, nil
}

func validateManifest(record manifestRecord) error {
	if record.Schema != schemaVersion || validateComponentRecord(record.Record, record.Generation) != nil ||
		len(record.Chunks) > maximumChunks(record.Record.Length) ||
		len(record.Chunks) != maximumChunks(record.Record.Length) {
		return corrupt("component materialization manifest fields are invalid")
	}
	for _, chunkDigest := range record.Chunks {
		if !validDigest(chunkDigest) {
			return corrupt("component materialization manifest chunk digest is invalid")
		}
	}
	return nil
}

func encodeChunk(record chunkRecord) ([]byte, error) {
	if validateChunk(record) != nil {
		return nil, errs.New(errs.KindValidationFailed, "component materialization chunk is invalid")
	}
	return encodeCanonical(record, "component materialization chunk")
}

func decodeChunk(value []byte) (chunkRecord, error) {
	record, err := decodeCanonical[chunkRecord](value, "component materialization chunk")
	if err != nil || validateChunk(record) != nil {
		clear(record.Content)
		return chunkRecord{}, corrupt("component materialization chunk is corrupt")
	}
	return record, nil
}

func validateChunk(record chunkRecord) error {
	if record.Schema != schemaVersion || record.EnvironmentID == "" || record.MaterializationID == "" ||
		len(record.Content) == 0 || len(record.Content) > chunkContentBytes ||
		record.Offset != uint64(record.Ordinal)*uint64(chunkContentBytes) || digest(record.Content) != record.SHA256 {
		return corrupt("component materialization chunk fields are invalid")
	}
	return nil
}

func validateComponentRecord(record taskmaterialization.Record, generation uint64) error {
	component := record.Source.ComponentFile
	if taskmaterialization.ValidateRecord(record, generation) != nil ||
		record.Source.Kind != taskmaterialization.SourceComponentFile || component == nil ||
		record.Source.BlueprintFile != nil || record.Source.EntryValue != nil ||
		record.Source.GeneratedEnvironment != nil || record.OutputKind != taskmaterialization.OutputPlainFile ||
		component.Path != record.Destination || !validDigest(record.SHA256) {
		return errs.New(errs.KindValidationFailed, "only non-secret Component plain-file content may be retained")
	}
	return nil
}

func encodeCanonical[T any](record T, name string) ([]byte, error) {
	value, err := json.Marshal(record)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(value) > maximumEncodedRecordBytes {
		clear(value)
		return nil, errs.New(errs.KindValidationFailed, name+" exceeds the durable record limit")
	}
	return value, nil
}

func decodeCanonical[T any](value []byte, name string) (T, error) {
	var zero T
	if len(value) == 0 || len(value) > maximumEncodedRecordBytes {
		return zero, corrupt(name + " is corrupt")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var record T
	if decoder.Decode(&record) != nil {
		return zero, corrupt(name + " is corrupt")
	}
	var trailing struct{}
	if decoder.Decode(&trailing) != io.EOF {
		return zero, corrupt(name + " has trailing data")
	}
	canonical, err := json.Marshal(record)
	if err != nil || !bytes.Equal(canonical, value) {
		return zero, corrupt(name + " is not canonical")
	}
	return record, nil
}

func maximumChunks(length uint64) int {
	return int((length + uint64(chunkContentBytes) - 1) / uint64(chunkContentBytes))
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func corrupt(message string) error { return errs.New(errs.KindInternal, message) }
