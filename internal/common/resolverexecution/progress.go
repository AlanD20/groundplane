package resolverexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type Stage string

const (
	WriteConfiguration  Stage = "write_configuration"
	ReconcileRuntime    Stage = "reconcile_runtime"
	VerifyDNS           Stage = "verify_dns"
	ApplyHostResolver   Stage = "apply_host_resolver"
	RestoreHostResolver Stage = "restore_host_resolver"
	RemoveRuntime       Stage = "remove_runtime"
	VerifyAbsent        Stage = "verify_absent"
	Complete            Stage = "complete"
)

// Layout binds the compact journal to its actual input without duplicating
// Corefile content, Compose YAML or DNS probe results at every checkpoint.
type Layout struct {
	Input               workexecution.Input `json:"input"`
	ComponentID         string              `json:"component_id"`
	Enabled             bool                `json:"enabled"`
	EnsureRuntime       bool                `json:"ensure_runtime"`
	PreviousEnabled     bool                `json:"previous_enabled"`
	ConfigurationSHA256 string              `json:"configuration_sha256,omitempty"`
}

func CaptureLayout(attempt workexecution.Attempt, input Input) (Layout, error) {
	_, reference, err := Capture(attempt.Input.ID, input)
	if err != nil || reference != attempt.Input || input.TaskID != attempt.TaskID || input.WorkID != attempt.WorkItemID ||
		input.HostID != attempt.Executor.HostID {
		return Layout{}, invalid("DNS resolver journal does not match its captured assignment")
	}
	layout := Layout{Input: reference, ComponentID: input.ComponentID, Enabled: input.Enabled, EnsureRuntime: input.EnsureRuntime, PreviousEnabled: input.PreviousEnabled}
	if input.Enabled {
		layout.ConfigurationSHA256 = hex.EncodeToString(input.Candidate.Apply.ArtifactDigest)
	}
	return layout, layout.Validate(attempt)
}

func (layout Layout) Validate(attempt workexecution.Attempt) error {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent || layout.Input != attempt.Input ||
		ids.Validate(ids.KindComponent, layout.ComponentID) != nil || !layout.Enabled && layout.EnsureRuntime ||
		layout.Enabled && !validDigest(layout.ConfigurationSHA256) || !layout.Enabled && layout.ConfigurationSHA256 != "" {
		return invalid("DNS resolver journal layout is invalid")
	}
	return nil
}

func (layout Layout) Stages() []Stage {
	if !layout.Enabled {
		return []Stage{RestoreHostResolver, RemoveRuntime, VerifyAbsent}
	}
	stages := []Stage{WriteConfiguration}
	if layout.EnsureRuntime {
		stages = append(stages, ReconcileRuntime)
	}
	return append(stages, VerifyDNS, ApplyHostResolver)
}

type Observation struct {
	Input               workexecution.Input `json:"input"`
	ComponentID         string              `json:"component_id"`
	Stage               Stage               `json:"stage"`
	ConfigurationSHA256 string              `json:"configuration_sha256,omitempty"`
	ServingProofSHA256  string              `json:"serving_proof_sha256,omitempty"`
	RuntimePresent      bool                `json:"runtime_present"`
	RuntimeAbsent       bool                `json:"runtime_absent"`
	HostResolver        string              `json:"host_resolver,omitempty"`
	At                  time.Time           `json:"at"`
}

func (observed Observation) Satisfies(stage Stage, layout Layout) bool {
	if observed.Input != layout.Input || observed.ComponentID != layout.ComponentID || observed.Stage != stage || !canonicalTime(observed.At) {
		return false
	}
	want := Observation{Input: layout.Input, ComponentID: layout.ComponentID, Stage: stage, At: observed.At}
	switch stage {
	case WriteConfiguration:
		want.ConfigurationSHA256 = layout.ConfigurationSHA256
	case ReconcileRuntime:
		want.RuntimePresent = true
	case VerifyDNS:
		if !validDigest(observed.ServingProofSHA256) {
			return false
		}
		want.RuntimePresent, want.ConfigurationSHA256, want.ServingProofSHA256 = true, layout.ConfigurationSHA256, observed.ServingProofSHA256
	case ApplyHostResolver:
		want.HostResolver = "local_resolver"
	case RestoreHostResolver:
		want.HostResolver = "baseline"
	case RemoveRuntime, VerifyAbsent:
		want.RuntimeAbsent = true
	default:
		return false
	}
	return want == observed
}

type Progress struct {
	AttemptID string       `json:"attempt_id"`
	Sequence  uint64       `json:"sequence"`
	Stage     Stage        `json:"stage"`
	InFlight  bool         `json:"in_flight"`
	BeganAt   *time.Time   `json:"began_at,omitempty"`
	UpdatedAt time.Time    `json:"updated_at"`
	Last      *Observation `json:"last,omitempty"`
}

func NewProgress(attempt workexecution.Attempt, layout Layout, at time.Time) (Progress, error) {
	progress := Progress{AttemptID: attempt.ID, Stage: layout.Stages()[0], UpdatedAt: at.UTC()}
	if !at.Before(attempt.Deadline) {
		return Progress{}, invalid("DNS resolver deadline expired before execution")
	}
	return progress, progress.Validate(attempt, layout)
}

func (progress Progress) Validate(attempt workexecution.Attempt, layout Layout) error {
	if layout.Validate(attempt) != nil || attempt.Direction != workexecution.Forward || attempt.StartedAt == nil ||
		progress.AttemptID != attempt.ID || !canonicalTime(progress.UpdatedAt) || progress.UpdatedAt.Before(*attempt.StartedAt) ||
		progress.InFlight != (progress.Sequence%2 == 1) || progress.InFlight != (progress.BeganAt != nil) {
		return invalid("DNS resolver checkpoint identity or timing is invalid")
	}
	stages := layout.Stages()
	if progress.Sequence > uint64(len(stages)*2) {
		return invalid("DNS resolver checkpoint exceeds its captured procedure")
	}
	index, stage := progress.Sequence/2, Complete
	if index < uint64(len(stages)) {
		stage = stages[index]
	}
	if progress.Stage != stage || (progress.Last != nil) != (index != 0) {
		return invalid("DNS resolver checkpoint stage is inconsistent")
	}
	if began := progress.BeganAt; began != nil && (!canonicalTime(*began) || began.Before(*attempt.StartedAt) ||
		!began.Before(attempt.Deadline) || began.After(progress.UpdatedAt)) {
		return invalid("DNS resolver stage has invalid start timing")
	}
	if last := progress.Last; last != nil && (!last.Satisfies(stages[index-1], layout) ||
		last.At.Before(*attempt.StartedAt) || last.At.After(progress.UpdatedAt)) {
		return invalid("DNS resolver checkpoint has no matching completed-stage observation")
	}
	return nil
}

func (progress Progress) Begin(attempt workexecution.Attempt, layout Layout, at time.Time) (Progress, error) {
	if progress.Validate(attempt, layout) != nil || progress.InFlight || progress.Stage == Complete || at.Before(progress.UpdatedAt) || !at.Before(attempt.Deadline) {
		return Progress{}, invalid("DNS resolver cannot begin this execution stage")
	}
	next, began := progress, at.UTC()
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = true, &began, began
	return next, next.Validate(attempt, layout)
}

func (progress Progress) Confirm(attempt workexecution.Attempt, layout Layout, observation Observation, at time.Time) (Progress, error) {
	if progress.Validate(attempt, layout) != nil || !progress.InFlight || at.Before(progress.UpdatedAt) || !at.Before(attempt.Deadline) ||
		observation.At.Before(*progress.BeganAt) || observation.At.After(at) || !observation.Satisfies(progress.Stage, layout) {
		return Progress{}, invalid("DNS resolver observation does not confirm its active stage")
	}
	next := progress
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt, next.Last, next.Stage = false, nil, at.UTC(), &observation, Complete
	if stages := layout.Stages(); next.Sequence/2 < uint64(len(stages)) {
		next.Stage = stages[next.Sequence/2]
	}
	return next, next.Validate(attempt, layout)
}

func StageID(workID string, stage Stage) (string, error) {
	at, err := ids.Timestamp(ids.KindWorkItem, workID)
	if err != nil {
		return "", err
	}
	switch stage {
	case WriteConfiguration, ReconcileRuntime, VerifyDNS, ApplyHostResolver, RestoreHostResolver, RemoveRuntime, VerifyAbsent:
		return ids.DeriveAt(ids.KindStep, at, workID, "dns-resolver/"+string(stage)), nil
	default:
		return "", invalid("DNS resolver stage has no executable identity")
	}
}

func EqualProgress(left, right Progress) bool {
	if (left.BeganAt == nil) != (right.BeganAt == nil) || (left.Last == nil) != (right.Last == nil) {
		return false
	}
	return left.AttemptID == right.AttemptID && left.Sequence == right.Sequence && left.Stage == right.Stage && left.InFlight == right.InFlight &&
		left.UpdatedAt.Equal(right.UpdatedAt) && (left.BeganAt == nil || left.BeganAt.Equal(*right.BeganAt)) && (left.Last == nil || *left.Last == *right.Last)
}

func ValidateTransition(attempt workexecution.Attempt, layout Layout, prior *Progress, next Progress) error {
	if err := next.Validate(attempt, layout); err != nil {
		return err
	}
	var expected Progress
	var err error
	if prior == nil {
		initial, initialErr := NewProgress(attempt, layout, next.UpdatedAt)
		if initialErr != nil {
			return initialErr
		}
		expected, err = initial.Begin(attempt, layout, next.UpdatedAt)
	} else if next.InFlight {
		expected, err = prior.Begin(attempt, layout, next.UpdatedAt)
	} else if next.Last != nil {
		expected, err = prior.Confirm(attempt, layout, *next.Last, next.UpdatedAt)
	} else {
		return invalid("DNS resolver confirmation is missing its observation")
	}
	if err != nil || !EqualProgress(expected, next) {
		return invalid("DNS resolver checkpoint skipped or changed a captured execution boundary")
	}
	return nil
}

func EncodeProgress(attempt workexecution.Attempt, layout Layout, progress Progress) ([]byte, error) {
	if err := progress.Validate(attempt, layout); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(progress)
	if err != nil {
		return nil, err
	}
	return jcs.Canonicalize(raw)
}

func DecodeProgress(attempt workexecution.Attempt, layout Layout, raw []byte) (Progress, error) {
	if len(raw) == 0 || len(raw) > MaximumReportBytes {
		return Progress{}, invalid("DNS resolver checkpoint is missing or oversized")
	}
	progress, err := jcs.Decode[Progress](raw)
	if err != nil {
		return Progress{}, invalid("DNS resolver checkpoint cannot be decoded")
	}
	canonical, err := EncodeProgress(attempt, layout, progress)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Progress{}, invalid("DNS resolver checkpoint is not canonical")
	}
	return progress, nil
}

func (progress Progress) Checkpoint(attempt workexecution.Attempt, layout Layout) (workexecution.Checkpoint, error) {
	raw, err := EncodeProgress(attempt, layout, progress)
	if err != nil || progress.Sequence == 0 {
		return workexecution.Checkpoint{}, invalid("DNS resolver checkpoint has no execution boundary")
	}
	stage := progress.Stage
	if !progress.InFlight {
		stage = layout.Stages()[progress.Sequence/2-1]
	}
	stageID, err := StageID(attempt.WorkItemID, stage)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(raw)
	return workexecution.Checkpoint{AttemptID: attempt.ID, Sequence: progress.Sequence, StageID: stageID,
		Evidence: workexecution.Input{ID: attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}, RecordedAt: progress.UpdatedAt}, nil
}

func validDigest(digest string) bool {
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == digest && digest != hex.EncodeToString(make([]byte, sha256.Size))
}
