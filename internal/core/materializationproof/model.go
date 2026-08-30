// Package materializationproof defines immutable, generation-matched evidence
// that one applied Environment materialization set is present or absent.
package materializationproof

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	Schema                  = uint8(1)
	MaximumMembers          = 4096
	MaximumSourcesPerMember = 4096
	MaximumSources          = 16384
)

type Outcome string

const (
	OutcomePresent Outcome = "present"
	OutcomeAbsent  Outcome = "absent"
)

type OutputKind string

const (
	OutputGeneratedEnvironment OutputKind = "generated_env"
	OutputPlainFile            OutputKind = "plain_file"
	OutputSecretFile           OutputKind = "secret_file"
)

type EntryStorage string

const (
	EntryStoragePlain  EntryStorage = "plain"
	EntryStorageSecret EntryStorage = "secret"
)

type EntryGenerationRecord struct {
	EntryID      string       `json:"entry_id"`
	GenerationID string       `json:"generation_id"`
	Storage      EntryStorage `json:"storage"`
	ValueSHA256  string       `json:"value_sha256"`
}

type ReusableSecretRecord struct {
	SecretID         string `json:"secret_id"`
	MetadataRevision int64  `json:"metadata_revision"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
}

type MemberRecord struct {
	MaterializationID        string                  `json:"materialization_id"`
	ServiceID                string                  `json:"service_id"`
	ServiceName              string                  `json:"service_name"`
	Destination              string                  `json:"destination"`
	OutputKind               OutputKind              `json:"output_kind"`
	Outcome                  Outcome                 `json:"outcome"`
	UID                      uint32                  `json:"uid"`
	GID                      uint32                  `json:"gid"`
	Mode                     uint32                  `json:"mode"`
	Length                   uint64                  `json:"length"`
	ContentSHA256            string                  `json:"content_sha256"`
	AbsenceOrOwnershipSHA256 string                  `json:"absence_or_ownership_sha256"`
	EntryGenerations         []EntryGenerationRecord `json:"entry_generations"`
	ReusableSecrets          []ReusableSecretRecord  `json:"reusable_secrets"`
}

type Input struct {
	EnvironmentID     string
	AppliedRevisionID string
	RenderGeneration  uint64
	ProducingTaskID   string
	Members           []MemberRecord
}

// Record is the validated persistence snapshot of a Proof. Mutating a Record
// never mutates the Proof that produced it.
type Record struct {
	Schema            uint8          `json:"schema"`
	EnvironmentID     string         `json:"environment_id"`
	AppliedRevisionID string         `json:"applied_revision_id"`
	RenderGeneration  uint64         `json:"render_generation"`
	ProducingTaskID   string         `json:"producing_task_id"`
	MemberCount       uint32         `json:"member_count"`
	CanonicalSHA256   string         `json:"canonical_sha256"`
	Members           []MemberRecord `json:"members"`
}

// Proof is immutable after construction. Its only slice is private and every
// exported projection returns a deep copy.
type Proof struct{ record Record }

func New(input Input) (Proof, error) {
	record := Record{
		Schema: Schema, EnvironmentID: input.EnvironmentID, AppliedRevisionID: input.AppliedRevisionID,
		RenderGeneration: input.RenderGeneration, ProducingTaskID: input.ProducingTaskID,
		Members: cloneMembers(input.Members),
	}
	canonicalize(&record)
	record.MemberCount = uint32(len(record.Members))
	if err := validateRecord(record, false); err != nil {
		return Proof{}, err
	}
	record.CanonicalSHA256 = canonicalDigest(record)
	return Proof{record: record}, nil
}

// Restore validates a storage snapshot without normalizing it. Non-canonical
// ordering, counts, or digests therefore fail closed at the persistence edge.
func Restore(record Record) (Proof, error) {
	copyOfRecord := cloneRecord(record)
	if err := validateRecord(copyOfRecord, true); err != nil {
		return Proof{}, err
	}
	if canonicalDigest(copyOfRecord) != copyOfRecord.CanonicalSHA256 {
		return Proof{}, validationError("materialization proof canonical digest does not match")
	}
	return Proof{record: copyOfRecord}, nil
}

func (proof Proof) Record() Record            { return cloneRecord(proof.record) }
func (proof Proof) EnvironmentID() string     { return proof.record.EnvironmentID }
func (proof Proof) AppliedRevisionID() string { return proof.record.AppliedRevisionID }
func (proof Proof) RenderGeneration() uint64  { return proof.record.RenderGeneration }
func (proof Proof) ProducingTaskID() string   { return proof.record.ProducingTaskID }
func (proof Proof) CanonicalSHA256() string   { return proof.record.CanonicalSHA256 }

func canonicalize(record *Record) {
	for index := range record.Members {
		member := &record.Members[index]
		sort.Slice(member.EntryGenerations, func(left, right int) bool {
			return member.EntryGenerations[left].EntryID < member.EntryGenerations[right].EntryID
		})
		sort.Slice(member.ReusableSecrets, func(left, right int) bool {
			return member.ReusableSecrets[left].SecretID < member.ReusableSecrets[right].SecretID
		})
	}
	sort.Slice(record.Members, func(left, right int) bool {
		return record.Members[left].MaterializationID < record.Members[right].MaterializationID
	})
}

func validateRecord(record Record, requireCanonical bool) error {
	if record.Schema != Schema || ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, record.AppliedRevisionID) != nil || record.RenderGeneration == 0 ||
		ids.Validate(ids.KindTask, record.ProducingTaskID) != nil {
		return validationError("materialization proof identity is invalid")
	}
	if len(record.Members) > MaximumMembers || record.MemberCount != uint32(len(record.Members)) {
		return validationError("materialization proof member count is invalid")
	}
	if requireCanonical && !validDigest(record.CanonicalSHA256) {
		return validationError("materialization proof canonical digest is invalid")
	}
	previousMaterialization := ""
	totalSources := 0
	entries := make(map[string]EntryGenerationRecord)
	secrets := make(map[string]ReusableSecretRecord)
	destinations := make(map[string]struct{}, len(record.Members))
	for _, member := range record.Members {
		if err := validateMember(record, member); err != nil {
			return err
		}
		if requireCanonical && member.MaterializationID <= previousMaterialization {
			return validationError("materialization proof members are not uniquely sorted")
		}
		if member.MaterializationID == previousMaterialization {
			return validationError("materialization proof member is duplicated")
		}
		destination := physicalDestinationIdentity(member)
		if _, exists := destinations[destination]; exists {
			return validationError("materialization proof destination is duplicated")
		}
		destinations[destination] = struct{}{}
		totalSources += len(member.EntryGenerations) + len(member.ReusableSecrets)
		if totalSources > MaximumSources {
			return validationError("materialization proof source count exceeds the limit")
		}
		for _, source := range member.EntryGenerations {
			if existing, found := entries[source.EntryID]; found && existing != source {
				return validationError("materialization proof Entry generation is inconsistent")
			}
			entries[source.EntryID] = source
		}
		for _, source := range member.ReusableSecrets {
			if existing, found := secrets[source.SecretID]; found && existing != source {
				return validationError("materialization proof Secret input is inconsistent")
			}
			secrets[source.SecretID] = source
		}
		previousMaterialization = member.MaterializationID
	}
	return nil
}

func validateMember(record Record, member MemberRecord) error {
	if ids.Validate(ids.KindConfig, member.MaterializationID) != nil ||
		(member.ServiceID != "" && ids.Validate(ids.KindService, member.ServiceID) != nil) {
		return validationError("materialization proof member identity is invalid")
	}
	if err := validateDestination(member.Destination); err != nil {
		return err
	}
	if member.UID == ^uint32(0) || member.GID == ^uint32(0) ||
		member.Length > entrymaterialization.MaximumContentBytes || !validDigest(member.ContentSHA256) ||
		!validDigest(member.AbsenceOrOwnershipSHA256) {
		return validationError("materialization proof member metadata is invalid")
	}
	if err := validateOutput(record, member); err != nil {
		return err
	}
	if member.Outcome != OutcomePresent && member.Outcome != OutcomeAbsent {
		return validationError("materialization proof outcome is invalid")
	}
	if member.Outcome == OutcomeAbsent && (member.Length != 0 || member.ContentSHA256 != emptySHA256()) {
		return validationError("absent materialization proof carries content")
	}
	if len(member.EntryGenerations) > MaximumSourcesPerMember || len(member.ReusableSecrets) > MaximumSourcesPerMember {
		return validationError("materialization proof member source count exceeds the limit")
	}
	previousEntry := ""
	for _, source := range member.EntryGenerations {
		if ids.Validate(ids.KindEnvEntry, source.EntryID) != nil ||
			ids.Validate(ids.KindConfig, source.GenerationID) != nil || !validDigest(source.ValueSHA256) ||
			(source.Storage != EntryStoragePlain && source.Storage != EntryStorageSecret) {
			return validationError("materialization proof Entry generation is invalid")
		}
		if source.EntryID <= previousEntry {
			return validationError("materialization proof Entry generations are not uniquely sorted")
		}
		previousEntry = source.EntryID
	}
	previousSecret := ""
	for _, source := range member.ReusableSecrets {
		if ids.Validate(ids.KindSecret, source.SecretID) != nil || source.MetadataRevision <= 0 ||
			!validDigest(source.CiphertextSHA256) {
			return validationError("materialization proof Secret input is invalid")
		}
		if source.SecretID <= previousSecret {
			return validationError("materialization proof Secret inputs are not uniquely sorted")
		}
		previousSecret = source.SecretID
	}
	return nil
}

func validateOutput(record Record, member MemberRecord) error {
	switch member.OutputKind {
	case OutputGeneratedEnvironment:
		if member.UID != 0 || member.GID != 0 || member.Mode != uint32(entrymaterialization.ModePrivate) {
			return validationError("generated environment proof metadata is invalid")
		}
		if (member.ServiceID == "") != (member.ServiceName == "") ||
			(member.ServiceName != "" && !core.ValidEnvironmentComposeName(member.ServiceName)) {
			return validationError("generated environment proof Service name is invalid")
		}
		want, err := entrymaterialization.GeneratedEnvDestination(record.EnvironmentID, member.ServiceName)
		if err != nil || member.Destination != want {
			return validationError("generated environment proof destination is invalid")
		}
	case OutputPlainFile:
		if member.ServiceName != "" || member.Mode != uint32(entrymaterialization.ModeReadOnly) {
			return validationError("plain file proof metadata is invalid")
		}
	case OutputSecretFile:
		if member.ServiceName != "" || member.Mode != uint32(entrymaterialization.ModePrivate) {
			return validationError("secret file proof metadata is invalid")
		}
	default:
		return validationError("materialization proof output kind is invalid")
	}
	return nil
}

// All closed materialization kinds resolve Destination below the same stable
// Environment directory. The kind controls policy, not a second namespace.
func physicalDestinationIdentity(member MemberRecord) string { return member.Destination }

func validateDestination(value string) error {
	if value == "" || len(value) > int(entrymaterialization.MaximumDestinationBytes) || !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') || strings.Contains(value, `\`) || strings.HasPrefix(value, "/") ||
		path.Clean(value) != value || value == "." {
		return validationError("materialization proof destination is invalid")
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || strings.HasPrefix(component, entrymaterialization.TemporaryPrefix) {
			return validationError("materialization proof destination is invalid")
		}
	}
	return nil
}

func canonicalDigest(record Record) string {
	encoded := make([]byte, 0, 256+len(record.Members)*256)
	encoded = append(encoded, "groundplane.materialization-proof.v1\x00"...)
	encoded = append(encoded, record.Schema)
	encoded = appendString(encoded, record.EnvironmentID)
	encoded = appendString(encoded, record.AppliedRevisionID)
	encoded = appendUint64(encoded, record.RenderGeneration)
	encoded = appendString(encoded, record.ProducingTaskID)
	encoded = appendUint32(encoded, record.MemberCount)
	for _, member := range record.Members {
		encoded = appendString(encoded, member.ServiceID)
		encoded = appendString(encoded, member.MaterializationID)
		encoded = appendString(encoded, member.ServiceName)
		encoded = appendString(encoded, member.Destination)
		encoded = appendString(encoded, string(member.OutputKind))
		encoded = appendString(encoded, string(member.Outcome))
		encoded = appendUint32(encoded, member.UID)
		encoded = appendUint32(encoded, member.GID)
		encoded = appendUint32(encoded, member.Mode)
		encoded = appendUint64(encoded, member.Length)
		encoded = appendString(encoded, member.ContentSHA256)
		encoded = appendString(encoded, member.AbsenceOrOwnershipSHA256)
		encoded = appendUint32(encoded, uint32(len(member.EntryGenerations)))
		for _, source := range member.EntryGenerations {
			encoded = appendString(encoded, source.EntryID)
			encoded = appendString(encoded, source.GenerationID)
			encoded = appendString(encoded, string(source.Storage))
			encoded = appendString(encoded, source.ValueSHA256)
		}
		encoded = appendUint32(encoded, uint32(len(member.ReusableSecrets)))
		for _, source := range member.ReusableSecrets {
			encoded = appendString(encoded, source.SecretID)
			encoded = appendUint64(encoded, uint64(source.MetadataRevision))
			encoded = appendString(encoded, source.CiphertextSHA256)
		}
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	return hex.EncodeToString(digest[:])
}

func appendString(target []byte, value string) []byte {
	target = appendUint32(target, uint32(len(value)))
	return append(target, value...)
}

func appendUint32(target []byte, value uint32) []byte {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	return append(target, encoded[:]...)
}

func appendUint64(target []byte, value uint64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	return append(target, encoded[:]...)
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func emptySHA256() string {
	digest := sha256.Sum256(nil)
	return hex.EncodeToString(digest[:])
}

func cloneRecord(source Record) Record {
	clone := source
	clone.Members = cloneMembers(source.Members)
	return clone
}

func cloneMembers(source []MemberRecord) []MemberRecord {
	clone := make([]MemberRecord, len(source))
	for index := range source {
		clone[index] = source[index]
		clone[index].EntryGenerations = append([]EntryGenerationRecord(nil), source[index].EntryGenerations...)
		clone[index].ReusableSecrets = append([]ReusableSecretRecord(nil), source[index].ReusableSecrets...)
	}
	return clone
}

func validationError(message string) error { return errs.New(errs.KindValidationFailed, message) }
