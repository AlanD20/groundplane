package releases

import (
	"bytes"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/oklog/ulid/v2"
)

const (
	ReleaseStagingPrefix             = "/v1/staging/releases/"
	releaseRenderInputStagingPrefix  = "/v1/staging/release-render-inputs/"
	releaseCheckpointStagingPrefix   = "/v1/staging/release-checkpoints/"
	releaseManifestStagingPrefix     = "/v1/staging/release-manifests/"
	releasePublicationPrefix         = "/v1/records/release-publications/"
	releaseOperationPrefix           = "/v1/records/release-operations/"
	releaseFenceSetPrefix            = "/v1/runtime/release-fence-sets/"
	releaseEnvironmentIndexPrefix    = "/v1/indexes/releases/by-environment/"
	releaseServiceIndexPrefix        = "/v1/indexes/releases/by-service/"
	releaseTerminalPrefix            = "/v1/records/release-terminal-summaries/"
	releaseRetentionPrefix           = "/v1/runtime/release-retention/"
	releaseProjectionPrefix          = "/v1/runtime/service-release-projections/"
	MaximumReleaseRenderInputBytes   = 256 << 10
	MaximumReleasePublicationMembers = 32
)

type releaseEnvelope[T any] struct {
	Schema int    `json:"schema"`
	Kind   string `json:"kind"`
	Data   T      `json:"data"`
}

type ReleaseStageMember struct {
	Intent      domain.Intent     `json:"intent"`
	RenderInput json.RawMessage   `json:"render_input"`
	Checkpoint  domain.Checkpoint `json:"checkpoint"`
}

type ReleaseStage struct {
	PublicationID string               `json:"publication_id"`
	OperationID   string               `json:"operation_id"`
	Members       []ReleaseStageMember `json:"members"`
	CreatedAt     time.Time            `json:"created_at"`
}

type ReleaseStagedManifest struct {
	PublicationID string                   `json:"publication_id"`
	OperationID   string                   `json:"operation_id"`
	Members       []ReleaseStagedMemberRef `json:"members"`
	Digest        string                   `json:"digest"`
	CreatedAt     time.Time                `json:"created_at"`
}

type ReleaseStagedMemberRef struct {
	ReleaseID        string `json:"release_id"`
	ServiceID        string `json:"service_id"`
	IntentDigest     string `json:"intent_digest"`
	RenderDigest     string `json:"render_digest"`
	CheckpointDigest string `json:"checkpoint_digest"`
}

type ReleasePublicationMarker struct {
	PublicationID              string                                   `json:"publication_id"`
	OperationID                string                                   `json:"operation_id"`
	ManifestDigest             string                                   `json:"manifest_digest"`
	CandidateReleaseDescriptor executionplan.CandidateReleaseDescriptor `json:"candidate_release_descriptor"`
	ExecutedComposeArtifact    []byte                                   `json:"executed_compose_artifact,omitempty"`
	PreparedRuntimes           []executionplan.CandidateRuntime         `json:"prepared_runtimes,omitempty"`
	BlueprintRuntimes          []executionplan.BlueprintRuntimeInput    `json:"blueprint_runtimes,omitempty"`
	NativePredecessors         []BlueprintNativePredecessorReference    `json:"native_predecessors,omitempty"`
	PublishedAt                time.Time                                `json:"published_at"`
}

type ReleaseFenceMember struct {
	ServiceID          string `json:"service_id"`
	CandidateReleaseID string `json:"candidate_release_id"`
	RenderInputDigest  string `json:"render_input_digest"`
}

type ReleaseFenceSet struct {
	EnvironmentID string               `json:"environment_id"`
	Generation    uint64               `json:"generation"`
	OperationID   string               `json:"operation_id"`
	AttemptTaskID string               `json:"attempt_task_id"`
	Group         bool                 `json:"group"`
	Members       []ReleaseFenceMember `json:"members"`
}

type ReleaseOperationHead struct {
	OperationID              string                `json:"operation_id"`
	PublicationID            string                `json:"publication_id"`
	EnvironmentID            string                `json:"environment_id"`
	ReleaseGroupID           string                `json:"release_group_id,omitempty"`
	FailurePolicy            domain.OnFailure      `json:"failure_policy"`
	State                    domain.State          `json:"state"`
	RecoveryOutcome          domain.State          `json:"recovery_outcome,omitempty"`
	FailedMemberOrdinal      uint32                `json:"failed_member_ordinal,omitempty"`
	Attempts                 []domain.Attempt      `json:"attempts"`
	Members                  []domain.GroupMember  `json:"members"`
	Progress                 *domain.GroupProgress `json:"progress,omitempty"`
	LatestTaskID             string                `json:"latest_task_id"`
	ConfiguredTimeoutSeconds int64                 `json:"configured_timeout_seconds"`
	ComputedBudgetSeconds    int64                 `json:"computed_budget_seconds"`
	CreatedAt                time.Time             `json:"created_at"`
	UpdatedAt                time.Time             `json:"updated_at"`
}

type ReleaseEnvironmentIndexValue struct {
	Schema        int    `json:"schema"`
	ServiceID     string `json:"service_id"`
	PublicationID string `json:"publication_id"`
}

type ReleaseServiceIndexValue struct {
	Schema        int    `json:"schema"`
	PublicationID string `json:"publication_id"`
}

func ReleaseIntentStagingKey(publicationID, releaseID string) string {
	_ = publicationID
	return ReleaseStagingPrefix + releaseID
}

func ReleaseRenderInputStagingKey(publicationID, releaseID string) string {
	_ = publicationID
	return releaseRenderInputStagingPrefix + releaseID
}

func ReleaseCheckpointStagingKey(publicationID, releaseID string) string {
	_ = publicationID
	return releaseCheckpointStagingPrefix + releaseID
}

func ReleaseManifestStagingKey(publicationID string) string {
	return releaseManifestStagingPrefix + publicationID
}

func ReleasePublicationKey(publicationID string) string {
	return releasePublicationPrefix + publicationID
}

func ReleaseOperationKey(operationID string) string  { return releaseOperationPrefix + operationID }
func ReleaseFenceSetKey(environmentID string) string { return releaseFenceSetPrefix + environmentID }

func ReleaseEnvironmentIndexKey(environmentID, releaseID string) string {
	return releaseEnvironmentIndexPrefix + environmentID + "/" + releaseID
}

func ReleaseEnvironmentIndexScope(environmentID string) string {
	return releaseEnvironmentIndexPrefix + environmentID + "/"
}

func ReleaseServiceIndexKey(environmentID, serviceID, releaseID string) string {
	return releaseServiceIndexPrefix + environmentID + "/" + serviceID + "/" + releaseID
}

func ReleaseServiceIndexScope(environmentID, serviceID string) string {
	return releaseServiceIndexPrefix + environmentID + "/" + serviceID + "/"
}

func ReleaseTerminalKey(releaseID string) string   { return releaseTerminalPrefix + releaseID }
func ReleaseRetentionKey(releaseID string) string  { return releaseRetentionPrefix + releaseID }
func ReleaseProjectionKey(serviceID string) string { return releaseProjectionPrefix + serviceID }

func ValidatePublicationID(value string) error {
	if len(value) != 26 || value != strings.ToUpper(value) {
		return errs.New(errs.KindValidationFailed, "release publication id is invalid")
	}
	if _, err := ulid.ParseStrict(value); err != nil {
		return errs.New(errs.KindValidationFailed, "release publication id is invalid")
	}
	return nil
}

func EncodeReleaseRecord[T any](kind string, value T) ([]byte, error) {
	encoded, err := json.Marshal(releaseEnvelope[T]{Schema: 1, Kind: kind, Data: value})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(encoded) > domain.MaximumRecordBytes {
		return nil, errs.Newf(errs.KindValidationFailed,
			"release durable record %s exceeds size limit: %d bytes, maximum %d",
			kind, len(encoded), domain.MaximumRecordBytes)
	}
	return encoded, nil
}

func DecodeReleaseRecord[T any](value []byte, kind string) (T, error) {
	var zero T
	if recordcodec.RejectDuplicateFields(value) != nil {
		return zero, CorruptReleaseRecord()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var envelope releaseEnvelope[T]
	if err := decoder.Decode(&envelope); err != nil || recordcodec.RequireEOF(decoder) != nil || envelope.Schema != 1 ||
		envelope.Kind != kind {
		return zero, CorruptReleaseRecord()
	}
	return envelope.Data, nil
}

func CloneReleaseStage(value ReleaseStage) ReleaseStage {
	value.Members = slices.Clone(value.Members)
	for index := range value.Members {
		value.Members[index].RenderInput = slices.Clone(value.Members[index].RenderInput)
		value.Members[index].Checkpoint = domain.CloneCheckpoint(value.Members[index].Checkpoint)
	}
	return value
}

func ValidateReleaseStage(value ReleaseStage) error {
	if ValidatePublicationID(value.PublicationID) != nil || ids.Validate(ids.KindOperation, value.OperationID) != nil ||
		len(value.Members) == 0 || len(value.Members) > MaximumReleasePublicationMembers || value.CreatedAt.IsZero() ||
		value.CreatedAt.Location() != time.UTC {
		return errs.New(errs.KindValidationFailed, "release staging input is invalid")
	}
	services := make(map[string]struct{}, len(value.Members))
	releases := make(map[string]struct{}, len(value.Members))
	for _, member := range value.Members {
		if err := domain.ValidateIntent(member.Intent); err != nil || member.Intent.OperationID != value.OperationID ||
			member.Checkpoint.ReleaseID != member.Intent.ID || member.Checkpoint.State != domain.StatePending ||
			domain.ValidateCheckpoint(member.Checkpoint) != nil || len(member.RenderInput) == 0 ||
			len(member.RenderInput) > MaximumReleaseRenderInputBytes || !json.Valid(member.RenderInput) {
			return errs.New(errs.KindValidationFailed, "release staged member is invalid")
		}
		renderDigest, err := domain.Digest(json.RawMessage(member.RenderInput))
		if err != nil || renderDigest != member.Intent.RenderInputDigest {
			return errs.New(errs.KindValidationFailed, "release render input digest does not match immutable intent")
		}
		if _, exists := services[member.Intent.ServiceID]; exists {
			return errs.New(errs.KindValidationFailed, "release staged service is duplicated")
		}
		if _, exists := releases[member.Intent.ID]; exists {
			return errs.New(errs.KindValidationFailed, "release staged candidate is duplicated")
		}
		services[member.Intent.ServiceID] = struct{}{}
		releases[member.Intent.ID] = struct{}{}
	}
	return nil
}

func ValidateReleaseFenceSet(value ReleaseFenceSet, head ReleaseOperationHead, manifest ReleaseStagedManifest) error {
	if value.EnvironmentID != head.EnvironmentID || value.Generation == 0 || value.OperationID != head.OperationID ||
		value.AttemptTaskID != head.LatestTaskID || value.Group != (head.ReleaseGroupID != "") || len(value.Members) != len(manifest.Members) {
		return errs.New(errs.KindValidationFailed, "release fence set is invalid")
	}
	sorted := slices.Clone(value.Members)
	slices.SortFunc(
		sorted,
		func(left, right ReleaseFenceMember) int { return strings.Compare(left.ServiceID, right.ServiceID) },
	)
	for index := range sorted {
		if index > 0 && sorted[index-1].ServiceID == sorted[index].ServiceID {
			return errs.New(errs.KindValidationFailed, "release fence member is duplicated")
		}
		found := false
		for _, staged := range manifest.Members {
			if staged.ServiceID == sorted[index].ServiceID && staged.ReleaseID == sorted[index].CandidateReleaseID &&
				staged.RenderDigest == sorted[index].RenderInputDigest {
				found = true
				break
			}
		}
		if !found {
			return errs.New(errs.KindValidationFailed, "release fence member does not match staging")
		}
	}
	return nil
}

func CorruptReleaseRecord() error {
	return errs.New(errs.KindInternal, "release durable record is corrupt")
}
