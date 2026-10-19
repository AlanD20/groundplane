package releaseexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RecoveryReport struct {
	Sequence uint64                    `json:"sequence"`
	StageID  string                    `json:"stage_id"`
	Progress RecoveryProgress          `json:"progress"`
	Ack      *workexecution.Checkpoint `json:"ack,omitempty"`
}

func (report RecoveryReport) Validate(input RecoveryInput) error {
	if err := input.Validate(); err != nil {
		return err
	}
	return report.ValidateEnvelope(input)
}

func (report RecoveryReport) ValidateEnvelope(input RecoveryInput) error {
	if report.Progress.validateEnvelope(input) != nil || report.Progress.Sequence == 0 ||
		report.Sequence < report.Progress.Sequence ||
		ids.Validate(ids.KindStep, report.StageID) != nil {
		return invalidProgress()
	}
	// A confirmation names the action just completed, not the next action.
	index := report.Progress.Sequence / 2
	if !report.Progress.InFlight {
		index--
	}
	expected, err := recoveryActionID(input.Input.Attempt.Input, index)
	if err != nil {
		return err
	}
	if expected != report.StageID {
		return invalidProgress()
	}
	if report.Ack != nil {
		checkpoint, err := report.Checkpoint(input)
		if err != nil || !EqualCheckpoint(checkpoint, *report.Ack) {
			return invalidProgress()
		}
	}
	return nil
}

func (report RecoveryReport) Checkpoint(input RecoveryInput) (workexecution.Checkpoint, error) {
	report.Ack = nil
	if err := report.ValidateEnvelope(input); err != nil {
		return workexecution.Checkpoint{}, err
	}
	raw, err := json.Marshal(struct {
		Attempt          workexecution.Attempt                  `json:"attempt"`
		ForwardAttemptID string                                 `json:"forward_attempt_id"`
		SourceResult     workexecution.Input                    `json:"source_result"`
		Group            *GroupRestorationAuthority             `json:"group,omitempty"`
		Interruption     *RestorationInterruption               `json:"interruption,omitempty"`
		Report           RecoveryReport                         `json:"report"`
		Disposition      agentpb.WorkReleaseRecoveryDisposition `json:"disposition"`
	}{input.Input.Attempt, input.Forward.ID, *input.Forward.Result, input.Group, input.Interruption, report, input.Disposition})
	if err != nil {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(canonical)
	return workexecution.Checkpoint{
		AttemptID:  input.Input.Attempt.ID,
		Sequence:   report.Sequence,
		StageID:    report.StageID,
		RecordedAt: report.Progress.UpdatedAt,
		Evidence:   workexecution.Input{ID: input.Input.Attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])},
	}, nil
}
