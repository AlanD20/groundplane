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
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	Prefix             = "/v1/software-activations/"
	recordKind         = "software_activation"
	TaskTimeoutSeconds = int64(3 * 60 * 60)
	maximumInputBytes  = 128 << 10
)

type Phase string

const (
	PhaseAccepted  Phase = "accepted"
	PhaseStaged    Phase = "staged"
	PhaseCompleted Phase = "completed"
	PhaseFailed    Phase = "failed"
)

// Input freezes the verified preparation and the enrolled Agent predecessor.
// StagingAgentImage preserves that Agent across a Controller activation; when
// no Agent is enrolled it is the explicit bootstrap/selected default only.
type Input struct {
	PreviousController upgrade.Digest            `json:"previous_controller,omitempty"`
	OperationID        string                    `json:"operation_id"`
	Preparation        preparationrecord.Record  `json:"preparation"`
	Agent              *upgrade.AgentPredecessor `json:"agent,omitempty"`
	StagingAgentImage  string                    `json:"staging_agent_image,omitempty"`
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
		if !input.PreviousController.Valid() || !imageref.IsDigestPinned(input.StagingAgentImage) ||
			input.Agent != nil && input.StagingAgentImage != input.Agent.Image {
			return errs.New(errs.KindValidationFailed, "Controller staging Agent image is invalid")
		}
	} else if input.StagingAgentImage != "" || input.PreviousController != "" {
		return errs.New(errs.KindValidationFailed, "Agent-only activation cannot stage a Controller image")
	}
	return nil
}

type Progress struct {
	Phase             Phase            `json:"phase"`
	StagedController  *upgrade.Release `json:"staged_controller,omitempty"`
	AgentStaged       bool             `json:"agent_staged"`
	ControllerApplied bool             `json:"controller_applied"`
	AgentApplied      bool             `json:"agent_applied"`
	ErrorCode         string           `json:"error_code,omitempty"`
	ErrorDetail       string           `json:"error_detail,omitempty"`
}

type Record struct {
	TaskID               string    `json:"task_id"`
	WorkItemID           string    `json:"work_item_id"`
	ControllerWorkItemID string    `json:"controller_work_item_id,omitempty"`
	AgentWorkItemID      string    `json:"agent_work_item_id,omitempty"`
	OperationID          string    `json:"operation_id"`
	InputSHA256          string    `json:"input_sha256"`
	Input                Input     `json:"input"`
	Progress             Progress  `json:"progress"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

func NewRecord(taskID, inputSHA256 string, input Input, at time.Time) (Record, error) {
	record := Record{TaskID: taskID, WorkItemID: ids.New(ids.KindWorkItem), OperationID: input.OperationID,
		InputSHA256: inputSHA256, Input: input, Progress: Progress{Phase: PhaseAccepted}, CreatedAt: at, UpdatedAt: at}
	if input.Preparation.Source.Selection.IncludesController() {
		record.ControllerWorkItemID = ids.New(ids.KindWorkItem)
	}
	if input.Preparation.Source.Selection.IncludesAgent() {
		record.AgentWorkItemID = ids.New(ids.KindWorkItem)
	}
	return record, ValidateRecord(record)
}

func Key(taskID string) string { return Prefix + taskID }

func EncodeInput(input Input) (string, string, error) {
	if err := input.Validate(); err != nil {
		return "", "", err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return "", "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(raw)
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return "", "", err
	}
	defer clear(canonical)
	if len(canonical) > maximumInputBytes {
		return "", "", errs.New(errs.KindValidationFailed, "software activation input is too large")
	}
	return string(canonical), fmt.Sprintf("%x", sha256.Sum256(canonical)), nil
}

func (record Record) Reference() (workexecution.Input, error) {
	_, hash, err := EncodeInput(record.Input)
	if err != nil {
		return workexecution.Input{}, err
	}
	if hash != record.InputSHA256 {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "software activation input digest changed")
	}
	return workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, record.CreatedAt, record.TaskID, "software-activation"), SHA256: hash}, nil
}

func ValidateRecord(record Record) error {
	if ids.Validate(ids.KindTask, record.TaskID) != nil || ids.Validate(ids.KindWorkItem, record.WorkItemID) != nil ||
		ids.Validate(ids.KindOperation, record.OperationID) != nil || record.OperationID != record.Input.OperationID ||
		!recordcodec.IsCanonicalUTC(record.CreatedAt) || !recordcodec.IsCanonicalUTC(record.UpdatedAt) || record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "software activation identity or timestamps are invalid")
	}
	selection := record.Input.Preparation.Source.Selection
	if selection.IncludesController() != (record.ControllerWorkItemID != "") || selection.IncludesAgent() != (record.AgentWorkItemID != "") ||
		record.ControllerWorkItemID != "" && ids.Validate(ids.KindWorkItem, record.ControllerWorkItemID) != nil ||
		record.AgentWorkItemID != "" && ids.Validate(ids.KindWorkItem, record.AgentWorkItemID) != nil ||
		record.WorkItemID == record.ControllerWorkItemID || record.WorkItemID == record.AgentWorkItemID ||
		record.ControllerWorkItemID != "" && record.ControllerWorkItemID == record.AgentWorkItemID {
		return errs.New(errs.KindValidationFailed, "software activation work membership is invalid")
	}
	if _, err := record.Reference(); err != nil {
		return err
	}
	return ValidateProgress(record.Progress, record.Input)
}

func ValidateProgress(progress Progress, input Input) error {
	selection := input.Preparation.Source.Selection
	if len(progress.ErrorCode) > 128 || len(progress.ErrorDetail) > 2048 ||
		progress.ControllerApplied && !selection.IncludesController() || progress.AgentApplied && !selection.IncludesAgent() ||
		progress.AgentStaged && !selection.IncludesAgent() ||
		progress.StagedController != nil && (!selection.IncludesController() || progress.StagedController.ValidateRecord() != nil ||
			string(progress.StagedController.Manifest.ControllerSHA256) != input.Preparation.Progress.Result.Controller.BinarySHA256 ||
			progress.StagedController.Manifest.AgentImage != input.StagingAgentImage) {
		return errs.New(errs.KindValidationFailed, "software activation output does not match captured components")
	}
	switch progress.Phase {
	case PhaseAccepted:
		if progress.StagedController != nil || progress.AgentStaged || progress.ControllerApplied || progress.AgentApplied || progress.ErrorCode != "" || progress.ErrorDetail != "" {
			return invalidProgress()
		}
	case PhaseStaged:
		if selection.IncludesController() != (progress.StagedController != nil) || selection.IncludesAgent() != progress.AgentStaged ||
			progress.ControllerApplied || progress.AgentApplied || progress.ErrorCode != "" || progress.ErrorDetail != "" {
			return invalidProgress()
		}
	case PhaseCompleted:
		if selection.IncludesController() != progress.ControllerApplied || selection.IncludesAgent() != progress.AgentApplied ||
			progress.ErrorCode != "" || progress.ErrorDetail != "" {
			return invalidProgress()
		}
	case PhaseFailed:
		if progress.ErrorCode == "" || progress.ErrorDetail == "" {
			return invalidProgress()
		}
	default:
		return invalidProgress()
	}
	return nil
}

func invalidProgress() error {
	return errs.New(errs.KindValidationFailed, "software activation progress is inconsistent")
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
		return Record{}, errs.New(errs.KindInternal, "software activation record is corrupt")
	}
	return record, nil
}
