// Package softwareactivation owns the durable projection for applying one
// previously verified software preparation.
package softwareactivation

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	Prefix               = "/v1/software-activations/"
	ClaimPrefix          = "/v1/runtime/software-activation-claims/"
	recordKind           = "software_activation"
	TaskTimeoutSeconds   = int64(3 * 60 * 60)
	MaximumPhaseFailures = uint8(3)
	maximumInputBytes    = 128 << 10
)

type Phase string

const (
	PhaseAccepted            Phase = "accepted"
	PhaseStagingController   Phase = "staging_controller"
	PhaseControllerStaged    Phase = "controller_staged"
	PhaseControllerPublished Phase = "controller_published"
	PhaseControllerApplied   Phase = "controller_applied"
	PhaseAgentPublished      Phase = "agent_published"
	PhaseCompleted           Phase = "completed"
	PhaseFailed              Phase = "failed"
)

// Input freezes the verified preparation and the enrolled Agent predecessor.
// StagingAgentImage preserves that Agent across a Controller activation; when
// no Agent is enrolled it is the explicit bootstrap/selected default only.
type Input struct {
	OperationID       string                    `json:"operation_id"`
	Preparation       preparationrecord.Record  `json:"preparation"`
	Agent             *upgrade.AgentPredecessor `json:"agent,omitempty"`
	StagingAgentImage string                    `json:"staging_agent_image,omitempty"`
}

func (input Input) Validate() error {
	if ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		preparationrecord.ValidateRecord(input.Preparation) != nil ||
		input.Preparation.Progress.Phase != preparation.PhaseVerified {
		return errs.New(errs.KindValidationFailed, "software activation input is not a verified preparation")
	}
	result := input.Preparation.Progress.Result
	selection := input.Preparation.Source.Selection
	if !result.Source.Equal(input.Preparation.Source) ||
		selection.IncludesController() != (result.Controller != nil) ||
		selection.IncludesAgent() != (result.Agent != nil) {
		return errs.New(errs.KindValidationFailed, "software activation outputs do not match their selection")
	}
	if input.Agent != nil {
		if ids.Validate(ids.KindAgent, input.Agent.ID) != nil || input.Agent.Generation == 0 ||
			input.Agent.Generation > math.MaxUint64-4 ||
			!imageref.IsDigestPinned(input.Agent.Image) {
			return errs.New(errs.KindValidationFailed, "software activation Agent predecessor is invalid")
		}
	}
	if selection.IncludesAgent() && input.Agent == nil {
		return errs.New(errs.KindValidationFailed, "Agent activation requires an enrolled Agent predecessor")
	}
	if selection.IncludesController() {
		if !imageref.IsDigestPinned(input.StagingAgentImage) ||
			input.Agent != nil && input.StagingAgentImage != input.Agent.Image {
			return errs.New(errs.KindValidationFailed, "Controller staging Agent image is invalid")
		}
	} else if input.StagingAgentImage != "" {
		return errs.New(errs.KindValidationFailed, "Agent-only activation cannot stage a Controller image")
	}
	return nil
}

type Progress struct {
	Phase             Phase            `json:"phase"`
	StagedController  *upgrade.Release `json:"staged_controller,omitempty"`
	ControllerTaskID  string           `json:"controller_task_id,omitempty"`
	AgentTaskID       string           `json:"agent_task_id,omitempty"`
	ControllerApplied bool             `json:"controller_applied,omitempty"`
	AgentApplied      bool             `json:"agent_applied,omitempty"`
	FailureCount      uint8            `json:"failure_count,omitempty"`
	RetryAt           time.Time        `json:"retry_at,omitempty"`
	ErrorCode         string           `json:"error_code,omitempty"`
	ErrorDetail       string           `json:"error_detail,omitempty"`
}

func (progress Progress) Equal(other Progress) bool {
	return progress.Phase == other.Phase && equalRelease(progress.StagedController, other.StagedController) &&
		progress.ControllerTaskID == other.ControllerTaskID && progress.AgentTaskID == other.AgentTaskID &&
		progress.ControllerApplied == other.ControllerApplied && progress.AgentApplied == other.AgentApplied &&
		progress.FailureCount == other.FailureCount && progress.RetryAt.Equal(other.RetryAt) &&
		progress.ErrorCode == other.ErrorCode && progress.ErrorDetail == other.ErrorDetail
}

func equalRelease(left, right *upgrade.Release) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Release == right.Release && left.Manifest == right.Manifest
}

type Record struct {
	TaskID      string    `json:"task_id"`
	OperationID string    `json:"operation_id"`
	InputSHA256 string    `json:"input_sha256"`
	Input       Input     `json:"input"`
	Progress    Progress  `json:"progress"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func NewRecord(taskID, inputSHA256 string, input Input, at time.Time) (Record, error) {
	record := Record{TaskID: taskID, OperationID: input.OperationID, InputSHA256: inputSHA256,
		Input: input, Progress: Progress{Phase: PhaseAccepted}, CreatedAt: at, UpdatedAt: at}
	return record, ValidateRecord(record)
}

func Key(taskID string) string      { return Prefix + taskID }
func ClaimKey(taskID string) string { return ClaimPrefix + taskID }

func EncodeInput(input Input) (string, string, error) {
	if err := input.Validate(); err != nil {
		return "", "", err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return "", "", errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return "", "", err
	}
	if len(canonical) > maximumInputBytes {
		return "", "", errs.New(errs.KindValidationFailed, "software activation input is too large")
	}
	return string(canonical), fmt.Sprintf("%x", sha256.Sum256(canonical)), nil
}

func DecodeInput(value, expectedSHA256 string) (Input, error) {
	raw := []byte(value)
	if len(raw) == 0 || len(raw) > maximumInputBytes ||
		fmt.Sprintf("%x", sha256.Sum256(raw)) != expectedSHA256 {
		return Input{}, errs.New(errs.KindValidationFailed, "software activation input hash is invalid")
	}
	input, err := jcs.Decode[Input](raw)
	if err != nil {
		return Input{}, err
	}
	return input, input.Validate()
}

func ValidateRecord(record Record) error {
	if ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindOperation, record.OperationID) != nil ||
		!recordcodec.ValidSHA256(record.InputSHA256) || record.OperationID != record.Input.OperationID {
		return errs.New(errs.KindValidationFailed, "software activation record identity is invalid")
	}
	if err := record.Input.Validate(); err != nil {
		return err
	}
	if err := ValidateProgress(record.Progress, record.Input); err != nil {
		return err
	}
	if !recordcodec.IsCanonicalUTC(record.CreatedAt) || !recordcodec.IsCanonicalUTC(record.UpdatedAt) ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "software activation timestamps are invalid")
	}
	return nil
}

func ValidateProgress(progress Progress, input Input) error {
	selection := input.Preparation.Source.Selection
	if progress.StagedController != nil && progress.StagedController.ValidateRecord() != nil ||
		progress.ControllerTaskID != "" && ids.Validate(ids.KindTask, progress.ControllerTaskID) != nil ||
		progress.AgentTaskID != "" && ids.Validate(ids.KindTask, progress.AgentTaskID) != nil ||
		progress.AgentApplied &&
			!selection.IncludesAgent() || progress.ControllerApplied && !selection.IncludesController() ||
		!progress.RetryAt.IsZero() && !recordcodec.IsCanonicalUTC(progress.RetryAt) ||
		progress.FailureCount > MaximumPhaseFailures ||
		len(progress.ErrorCode) > 128 || len(progress.ErrorDetail) > 2048 {
		return errs.New(errs.KindValidationFailed, "software activation progress is invalid")
	}
	controllerStaged := progress.StagedController != nil
	switch progress.Phase {
	case PhaseAccepted:
		if controllerStaged || progress.ControllerTaskID != "" || progress.AgentTaskID != "" ||
			progress.ControllerApplied || progress.AgentApplied {
			return invalidProgress()
		}
	case PhaseStagingController:
		if !selection.IncludesController() || controllerStaged || progress.ControllerTaskID != "" ||
			progress.AgentTaskID != "" || progress.ControllerApplied || progress.AgentApplied {
			return invalidProgress()
		}
	case PhaseControllerStaged:
		if !selection.IncludesController() || !controllerStaged || progress.ControllerTaskID != "" ||
			progress.AgentTaskID != "" || progress.ControllerApplied || progress.AgentApplied {
			return invalidProgress()
		}
	case PhaseControllerPublished:
		if !selection.IncludesController() || !controllerStaged || progress.ControllerTaskID == "" ||
			progress.AgentTaskID != "" || progress.ControllerApplied || progress.AgentApplied {
			return invalidProgress()
		}
	case PhaseControllerApplied:
		if !selection.IncludesController() || !controllerStaged || progress.ControllerTaskID == "" ||
			!progress.ControllerApplied || progress.AgentTaskID != "" || progress.AgentApplied {
			return invalidProgress()
		}
	case PhaseAgentPublished:
		if !selection.IncludesAgent() || progress.AgentTaskID == "" || progress.AgentApplied ||
			(selection.IncludesController() && (!controllerStaged || progress.ControllerTaskID == "" ||
				!progress.ControllerApplied)) {
			return invalidProgress()
		}
	case PhaseCompleted:
		if selection.IncludesController() != progress.ControllerApplied ||
			selection.IncludesAgent() != progress.AgentApplied ||
			progress.FailureCount != 0 ||
			!progress.RetryAt.IsZero() ||
			progress.ErrorCode != "" ||
			progress.ErrorDetail != "" {
			return invalidProgress()
		}
	case PhaseFailed:
		if progress.ErrorCode == "" || progress.ErrorDetail == "" || progress.AgentApplied ||
			progress.FailureCount == 0 || !progress.RetryAt.IsZero() {
			return invalidProgress()
		}
	default:
		return invalidProgress()
	}
	if progress.Phase != PhaseFailed && (progress.FailureCount == 0 != progress.RetryAt.IsZero() ||
		progress.FailureCount == 0 && (progress.ErrorCode != "" || progress.ErrorDetail != "") ||
		progress.FailureCount > 0 && (progress.ErrorCode == "" || progress.ErrorDetail == "") ||
		progress.FailureCount >= MaximumPhaseFailures) {
		return invalidProgress()
	}
	return nil
}

func invalidProgress() error {
	return errs.New(errs.KindValidationFailed, "software activation progress phase is inconsistent")
}

func ValidateTransition(current, next Progress, input Input) error {
	if err := ValidateProgress(next, input); err != nil {
		return err
	}
	if current.Phase == PhaseCompleted || current.Phase == PhaseFailed {
		if !current.Equal(next) {
			return errs.New(errs.KindStateConflict, "terminal software activation progress changed")
		}
		return nil
	}
	if !validPhaseTransition(current.Phase, next.Phase) {
		return errs.New(errs.KindStateConflict, "software activation progress regressed")
	}
	if current.StagedController != nil && !equalRelease(current.StagedController, next.StagedController) ||
		current.ControllerTaskID != "" && current.ControllerTaskID != next.ControllerTaskID ||
		current.AgentTaskID != "" && current.AgentTaskID != next.AgentTaskID ||
		current.ControllerApplied && !next.ControllerApplied || current.AgentApplied && !next.AgentApplied {
		return errs.New(errs.KindStateConflict, "software activation checkpoint authority changed")
	}
	return nil
}

func validPhaseTransition(current, next Phase) bool {
	if current == next || next == PhaseFailed {
		return true
	}
	switch current {
	case PhaseAccepted:
		return next == PhaseStagingController || next == PhaseAgentPublished
	case PhaseStagingController:
		return next == PhaseControllerStaged
	case PhaseControllerStaged:
		return next == PhaseControllerPublished
	case PhaseControllerPublished:
		return next == PhaseControllerApplied
	case PhaseControllerApplied:
		return next == PhaseAgentPublished || next == PhaseCompleted
	case PhaseAgentPublished:
		return next == PhaseCompleted
	default:
		return false
	}
}

func Encode(record Record) ([]byte, error) {
	if err := ValidateRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode(recordKind, record)
}

func Decode(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, recordKind)
	if err != nil || ValidateRecord(record) != nil {
		return Record{}, errs.New(errs.KindInternal, "durable software activation record is corrupt")
	}
	return record, nil
}
