// Package imageoperations owns captured image operations independently of Task history.
package imageoperations

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const MaximumBytes = 32 << 10

type Phase string

const (
	Ready     Phase = "ready"
	Fetching  Phase = "fetching"
	Tagging   Phase = "tagging"
	Removing  Phase = "removing"
	Uncertain Phase = "uncertain"
	Finished  Phase = "finished"
)

type Input struct {
	TaskID      string                  `json:"task_id"`
	WorkItemID  string                  `json:"work_item_id"`
	PlanID      string                  `json:"plan_id"`
	OperationID string                  `json:"operation_id"`
	Placement   workexecution.Placement `json:"placement"`
	Action      workexecution.Action    `json:"action"`
	Fetch       *imagefetch.Plan        `json:"fetch,omitempty"`
	RemoveID    string                  `json:"remove_id,omitempty"`
	CreatedAt   time.Time               `json:"created_at"`
}

// A tag or removal write may outlive a lost response. Uncertain retains its
// operation fence until inspection proves the effect; it is not a retry grant.
type Record struct {
	Input          Input                 `json:"input"`
	Phase          Phase                 `json:"phase"`
	UncertainPhase Phase                 `json:"uncertain_phase,omitempty"`
	AttemptID      string                `json:"attempt_id,omitempty"`
	ImageID        string                `json:"image_id,omitempty"`
	Outcome        workexecution.Outcome `json:"outcome,omitempty"`
	UpdatedAt      time.Time             `json:"updated_at"`
	FinishedAt     *time.Time            `json:"finished_at,omitempty"`
}

func (input Input) Validate() error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		input.Placement.Validate() != nil || !recordcodec.IsCanonicalUTC(input.CreatedAt) {
		return invalid()
	}
	switch input.Action {
	case workexecution.ActionFetch:
		if input.Fetch == nil || input.Fetch.Validate() != nil || input.RemoveID != "" {
			return invalid()
		}
	case workexecution.ActionRemove:
		if input.Fetch != nil {
			return invalid()
		}
		if _, err := imagefetch.RemovalHash(input.RemoveID); err != nil {
			return invalid()
		}
	default:
		return invalid()
	}
	return nil
}

func (input Input) Reference() (workexecution.Input, error) {
	if err := input.Validate(); err != nil {
		return workexecution.Input{}, err
	}
	value, err := json.Marshal(input)
	if err != nil {
		return workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	return workexecution.Input{ID: input.PlanID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func (input Input) Target() string {
	if input.Fetch != nil {
		return input.Fetch.ConfigDigest
	}
	return input.RemoveID
}

func (record Record) Validate() error {
	if record.Phase != Uncertain && record.UncertainPhase != "" {
		return invalid()
	}
	if record.Input.Validate() != nil || !recordcodec.IsCanonicalUTC(record.UpdatedAt) ||
		record.UpdatedAt.Before(record.Input.CreatedAt) {
		return invalid()
	}
	if record.AttemptID != "" && ids.Validate(ids.KindAttempt, record.AttemptID) != nil {
		return invalid()
	}
	if record.ImageID != "" && (record.Input.Fetch == nil || record.ImageID != record.Input.Fetch.ConfigDigest) {
		return invalid()
	}
	if record.Phase == Finished {
		if record.FinishedAt == nil || !record.FinishedAt.Equal(record.UpdatedAt) {
			return invalid()
		}
		switch record.Outcome {
		case workexecution.Succeeded:
			if record.AttemptID == "" ||
				record.Input.Fetch != nil && record.ImageID != record.Input.Fetch.ConfigDigest {
				return invalid()
			}
		case workexecution.Failed, workexecution.Cancelled, workexecution.Superseded:
		default:
			return invalid()
		}
		return nil
	}
	if record.FinishedAt != nil || record.Outcome != "" {
		return invalid()
	}
	switch record.Phase {
	case Ready:
		if record.AttemptID != "" || record.ImageID != "" {
			return invalid()
		}
	case Fetching:
		if record.AttemptID == "" || record.Input.Fetch == nil {
			return invalid()
		}
	case Tagging:
		if record.AttemptID == "" || record.Input.Fetch == nil || record.ImageID == "" {
			return invalid()
		}
		tag, err := record.Input.Fetch.TagReference()
		if err != nil || tag == "" {
			return invalid()
		}
	case Removing:
		if record.AttemptID == "" || record.Input.Action != workexecution.ActionRemove {
			return invalid()
		}
	case Uncertain:
		if record.AttemptID == "" ||
			record.UncertainPhase != Fetching && record.UncertainPhase != Tagging && record.UncertainPhase != Removing {
			return invalid()
		}
		previous := record
		previous.Phase, previous.UncertainPhase = record.UncertainPhase, ""
		return previous.Validate()
	default:
		return invalid()
	}
	return nil
}

func Encode(record Record) ([]byte, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("image_operation", record)
	if err != nil {
		return nil, err
	}
	if len(value) > MaximumBytes {
		return nil, invalid()
	}
	return value, nil
}

func Decode(value []byte) (Record, error) {
	if len(value) > MaximumBytes {
		return Record{}, invalid()
	}
	record, err := recordcodec.Decode[Record](value, "image_operation")
	if err != nil || record.Validate() != nil {
		return Record{}, errs.New(errs.KindInternal, "image operation record is corrupt")
	}
	return record, nil
}

func invalid() error {
	return errs.New(errs.KindValidationFailed, "image operation input or execution state is invalid")
}
