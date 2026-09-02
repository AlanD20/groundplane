package scriptsourcereference

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
)

const (
	PreparationPrefix      = "/v1/staging/script-operation-source-sets/"
	RootPrefix             = "/v1/records/script-operation-source-sets/"
	ForwardReferencePrefix = "/v1/runtime/script-source-references/"
	CountPrefix            = "/v1/runtime/script-source-counts/"
	preparationBatchSize   = 9
	releaseBatchSize       = 11
)

type SourceKind string

const (
	SourceBody            SourceKind = "body"
	SourceRunnerSnapshot  SourceKind = "runner_snapshot"
	SourceService         SourceKind = "service"
	SourceRelease         SourceKind = "release"
	SourceNetwork         SourceKind = "network"
	SourceVolume          SourceKind = "volume"
	SourceEntryValue      SourceKind = "entry_value"
	SourceSecretValue     SourceKind = "secret_value"
	SourceMaterialization SourceKind = "materialization"
)

type SourceIdentity struct {
	Kind                SourceKind `json:"kind"`
	EnvironmentID       string     `json:"environment_id,omitempty"`
	ScriptSetGeneration string     `json:"script_set_generation,omitempty"`
	ScriptID            string     `json:"script_id,omitempty"`
	BodyGeneration      uint64     `json:"body_generation,omitempty"`
	SnapshotID          string     `json:"snapshot_id,omitempty"`
	ServiceID           string     `json:"service_id,omitempty"`
	ReleaseID           string     `json:"release_id,omitempty"`
	NetworkID           string     `json:"network_id,omitempty"`
	VolumeID            string     `json:"volume_id,omitempty"`
	EntryID             string     `json:"entry_id,omitempty"`
	SecretID            string     `json:"secret_id,omitempty"`
	ValueGenerationID   string     `json:"value_generation_id,omitempty"`
	MaterializationID   string     `json:"materialization_id,omitempty"`
	RenderGeneration    uint64     `json:"render_generation,omitempty"`
}

type Reference struct {
	OperationID       string         `json:"operation_id"`
	ScriptExecutionID string         `json:"script_execution_id"`
	Source            SourceIdentity `json:"source"`
	SourceOwnerID     string         `json:"source_owner_id"`
	SourceModRevision int64          `json:"source_mod_revision"`
	SourceDigest      string         `json:"source_digest"`
}

type EvidenceMode string

const (
	EvidenceExisting EvidenceMode = "existing"
	EvidenceStaged   EvidenceMode = "staged"
)

type Member struct {
	Reference   Reference
	SourceKey   string
	Mode        EvidenceMode
	Stage       StageIdentity
	StagedValue []byte
}

type StageIdentity struct {
	EnvironmentID        string `json:"environment_id"`
	RevisionID           string `json:"revision_id"`
	RenderGeneration     uint64 `json:"render_generation"`
	FixedReadRevision    int64  `json:"fixed_read_revision"`
	CanonicalValueSHA256 string `json:"canonical_value_sha256"`
}

type Count struct {
	Source                   SourceIdentity `json:"source"`
	ReferencedExecutionCount uint64         `json:"referenced_execution_count"`
}

type PreparationPhase string

const (
	PreparationPreparing  PreparationPhase = "preparing"
	PreparationSealed     PreparationPhase = "sealed"
	PreparationAbandoning PreparationPhase = "abandoning"
)

type Preparation struct {
	OperationID       string           `json:"operation_id"`
	MembershipCount   uint64           `json:"membership_count"`
	MembershipSHA256  string           `json:"membership_sha256"`
	Phase             PreparationPhase `json:"phase"`
	PreparationCursor uint64           `json:"preparation_cursor"`
	ReleaseCursor     uint64           `json:"release_cursor"`
}

type OperationSourceRoot struct {
	OperationID      string `json:"operation_id"`
	MembershipCount  uint64 `json:"membership_count"`
	MembershipSHA256 string `json:"membership_sha256"`
	Phase            string `json:"phase"`
	ReleasePath      string `json:"release_path"`
	ReleaseCursor    uint64 `json:"release_cursor"`
}

type StagedRequirement struct {
	Source        SourceIdentity
	SourceKey     string
	SourceOwnerID string
	SourceDigest  string
	Stage         StageIdentity
	Value         []byte
}

type Prepared struct {
	operationID        string
	descriptorRevision int64
	membershipCount    uint64
	membershipSHA256   string
	staged             []StagedRequirement
}

func (prepared Prepared) IsZero() bool              { return prepared.operationID == "" }
func (prepared Prepared) OperationID() string       { return prepared.operationID }
func (prepared Prepared) DescriptorRevision() int64 { return prepared.descriptorRevision }
func (prepared Prepared) MembershipCount() uint64   { return prepared.membershipCount }
func (prepared Prepared) MembershipSHA256() string  { return prepared.membershipSHA256 }

type PublicationFragment struct {
	Conditions         []Condition
	Mutations          []Mutation
	StagedRequirements []StagedRequirement
}

func (fragment *PublicationFragment) Clear() {
	if fragment == nil {
		return
	}
	clearMutations(fragment.Mutations)
	for index := range fragment.StagedRequirements {
		clear(fragment.StagedRequirements[index].Value)
	}
	*fragment = PublicationFragment{}
}

func PreparationKey(operationID string) string { return PreparationPrefix + operationID }
func RootKey(operationID string) string        { return RootPrefix + operationID + "/root" }
func ReversePrefix(operationID string) string {
	return RootPrefix + operationID + "/executions/"
}
func ForwardKey(reference Reference) string {
	return ForwardReferencePrefix + SourceSuffix(reference.Source) + "/" + reference.OperationID + "/" + reference.ScriptExecutionID
}
func ReverseKey(reference Reference) string {
	return ReversePrefix(reference.OperationID) + reference.ScriptExecutionID + "/" + SourceSuffix(reference.Source)
}
func CountKey(source SourceIdentity) string { return CountPrefix + SourceSuffix(source) }
func ScriptPrimaryKey(source SourceIdentity) string {
	return "/v1/records/script-sets/" + source.EnvironmentID + "/generations/" +
		source.ScriptSetGeneration + "/scripts/" + source.ScriptID
}

func SourceSuffix(source SourceIdentity) string {
	switch source.Kind {
	case SourceBody:
		return "body/" + source.EnvironmentID + "/~" + base64.RawURLEncoding.EncodeToString([]byte(source.ScriptSetGeneration)) + "/" + source.ScriptID + "/" + strconv.FormatUint(source.BodyGeneration, 10)
	case SourceRunnerSnapshot:
		return "runner-snapshot/" + source.SnapshotID
	case SourceService:
		return "service/" + source.ServiceID
	case SourceRelease:
		return "release/" + source.ReleaseID
	case SourceNetwork:
		return "network/" + source.NetworkID
	case SourceVolume:
		return "volume/" + source.VolumeID
	case SourceEntryValue:
		return "entry-value/" + source.EntryID + "/" + source.ValueGenerationID
	case SourceSecretValue:
		return "secret-value/" + source.SecretID + "/" + source.ValueGenerationID
	case SourceMaterialization:
		return "materialization/" + source.MaterializationID + "/" + strconv.FormatUint(source.RenderGeneration, 10)
	default:
		return ""
	}
}

func canonicalMembers(operationID string, input []Member) ([]Member, string, []StagedRequirement, error) {
	if operationID == "" || len(input) == 0 {
		return nil, "", nil, validation("source preparation is empty")
	}
	members := make([]Member, len(input))
	for index, member := range input {
		member.StagedValue = append([]byte(nil), member.StagedValue...)
		if err := validateMember(operationID, member); err != nil {
			return nil, "", nil, err
		}
		members[index] = member
	}
	sort.Slice(members, func(left, right int) bool {
		leftSuffix, rightSuffix := SourceSuffix(members[left].Reference.Source), SourceSuffix(members[right].Reference.Source)
		if leftSuffix != rightSuffix {
			return leftSuffix < rightSuffix
		}
		return members[left].Reference.ScriptExecutionID < members[right].Reference.ScriptExecutionID
	})
	canonical := members[:0]
	bySource := make(map[string]Member)
	for _, member := range members {
		suffix := SourceSuffix(member.Reference.Source)
		if prior, exists := bySource[suffix]; exists && !sameSourceEvidence(prior, member) {
			return nil, "", nil, validation("source identity has conflicting evidence")
		}
		bySource[suffix] = member
		if len(canonical) != 0 && ReverseKey(canonical[len(canonical)-1].Reference) == ReverseKey(member.Reference) {
			if !sameMember(canonical[len(canonical)-1], member) {
				return nil, "", nil, validation("source membership conflicts")
			}
			continue
		}
		canonical = append(canonical, member)
	}
	hash := sha256.New()
	var length [8]byte
	stagedByKey := make(map[string]StagedRequirement)
	for _, member := range canonical {
		value, err := encodeCanonicalMember(member)
		if err != nil {
			return nil, "", nil, err
		}
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(value)
		clear(value)
		if member.Mode == EvidenceStaged {
			requirement := StagedRequirement{
				Source: member.Reference.Source, SourceKey: member.SourceKey,
				SourceOwnerID: member.Reference.SourceOwnerID, SourceDigest: member.Reference.SourceDigest,
				Stage: member.Stage,
				Value: append([]byte(nil), member.StagedValue...),
			}
			if prior, exists := stagedByKey[member.SourceKey]; exists {
				if SourceSuffix(prior.Source) != SourceSuffix(requirement.Source) ||
					prior.SourceOwnerID != requirement.SourceOwnerID || prior.SourceDigest != requirement.SourceDigest ||
					prior.Stage != requirement.Stage || !bytes.Equal(prior.Value, requirement.Value) {
					return nil, "", nil, validation("staged source key has conflicting canonical authority")
				}
				continue
			}
			stagedByKey[member.SourceKey] = requirement
		}
	}
	stagedKeys := make([]string, 0, len(stagedByKey))
	for key := range stagedByKey {
		stagedKeys = append(stagedKeys, key)
	}
	sort.Strings(stagedKeys)
	staged := make([]StagedRequirement, 0, len(stagedKeys))
	for _, key := range stagedKeys {
		staged = append(staged, stagedByKey[key])
	}
	return canonical, hex.EncodeToString(hash.Sum(nil)), staged, nil
}

func validateMember(operationID string, member Member) error {
	reference := member.Reference
	if reference.OperationID != operationID || reference.ScriptExecutionID == "" || SourceSuffix(reference.Source) == "" ||
		reference.SourceOwnerID == "" || member.SourceKey == "" || member.SourceKey[0] != '/' {
		return validation("source member is invalid")
	}
	switch member.Mode {
	case EvidenceExisting:
		if reference.SourceModRevision <= 0 || len(member.StagedValue) != 0 || member.Stage != (StageIdentity{}) {
			return validation("existing source evidence is invalid")
		}
	case EvidenceStaged:
		digest := sha256.Sum256(member.StagedValue)
		if reference.SourceModRevision != 0 || len(member.StagedValue) == 0 || member.Stage.EnvironmentID == "" ||
			member.Stage.RevisionID == "" || member.Stage.RenderGeneration == 0 || member.Stage.FixedReadRevision <= 0 ||
			member.Stage.CanonicalValueSHA256 != hex.EncodeToString(digest[:]) {
			return validation("staged source evidence is invalid")
		}
	default:
		return validation("source evidence mode is invalid")
	}
	return nil
}

func sameSourceEvidence(left, right Member) bool {
	return left.SourceKey == right.SourceKey && left.Mode == right.Mode &&
		left.Stage == right.Stage &&
		left.Reference.SourceOwnerID == right.Reference.SourceOwnerID &&
		left.Reference.SourceModRevision == right.Reference.SourceModRevision &&
		left.Reference.SourceDigest == right.Reference.SourceDigest && bytesEqual(left.StagedValue, right.StagedValue)
}

func sameMember(left, right Member) bool {
	return left.Reference == right.Reference && sameSourceEvidence(left, right)
}

func encodeCanonicalMember(member Member) ([]byte, error) {
	valueDigest := ""
	if len(member.StagedValue) != 0 {
		digest := sha256.Sum256(member.StagedValue)
		valueDigest = hex.EncodeToString(digest[:])
	}
	return json.Marshal(struct {
		Reference   Reference    `json:"reference"`
		SourceKey   string       `json:"source_key"`
		Mode        EvidenceMode `json:"mode"`
		Stage       StageIdentity `json:"stage,omitempty"`
		ValueSHA256 string        `json:"staged_value_sha256,omitempty"`
	}{member.Reference, member.SourceKey, member.Mode, member.Stage, valueDigest})
}

type envelope[T any] struct {
	Schema int    `json:"schema"`
	Kind   string `json:"kind"`
	Data   T      `json:"data"`
}

func encode[T any](kind string, value T) ([]byte, error) {
	return json.Marshal(envelope[T]{Schema: 1, Kind: kind, Data: value})
}

func decode[T any](kind string, value []byte) (T, error) {
	var result envelope[T]
	if err := json.Unmarshal(value, &result); err != nil || result.Schema != 1 || result.Kind != kind {
		var zero T
		return zero, corruption("durable envelope is invalid")
	}
	canonical, err := json.Marshal(result)
	if err != nil || !bytesEqual(canonical, value) {
		var zero T
		return zero, corruption("durable envelope is not canonical")
	}
	return result.Data, nil
}

func encodeReference(value Reference) ([]byte, error) {
	return encode("script-source-reference", value)
}
func decodeReference(value []byte) (Reference, error) {
	return decode[Reference]("script-source-reference", value)
}
func encodeCount(value Count) ([]byte, error) { return encode("script-source-count", value) }
func decodeCount(value []byte) (Count, error) { return decode[Count]("script-source-count", value) }
func encodePreparation(value Preparation) ([]byte, error) {
	return encode("script-source-preparation", value)
}
func decodePreparation(value []byte) (Preparation, error) {
	return decode[Preparation]("script-source-preparation", value)
}
func encodeRoot(value OperationSourceRoot) ([]byte, error) {
	return encode("script-operation-source-root", value)
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func cloneMutations(input []Mutation) []Mutation {
	result := make([]Mutation, len(input))
	for index, mutation := range input {
		result[index] = mutation
		result[index].Value = append([]byte(nil), mutation.Value...)
	}
	return result
}

func clearMutations(input []Mutation) {
	for index := range input {
		clear(input[index].Value)
	}
}
