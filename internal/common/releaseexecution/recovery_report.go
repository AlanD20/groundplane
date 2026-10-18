package releaseexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type RecoveryReport struct {
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
	if report.Progress.validateEnvelope(input) != nil || report.Progress.Sequence == 0 || ids.Validate(ids.KindStep, report.StageID) != nil {
		return invalidProgress()
	}
	phase := report.Progress.Phase
	// A confirmation names the action just completed, not the next action.
	if !report.Progress.InFlight {
		switch phase {
		case InspectFailedRuntime:
			phase = QuiesceForward
		case RestoreConfiguration:
			phase = InspectFailedRuntime
		case RestoreServingRuntime:
			phase = RestoreConfiguration
		case RemoveFailedCandidate:
			phase = RestoreServingRuntime
		case VerifyRestoration:
			phase = RemoveFailedCandidate
		case RecoveryComplete:
			phase = VerifyRestoration
		default:
			return invalidProgress()
		}
	}
	expected, err := RecoveryStageID(input.Input.Attempt.Input, phase)
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
		Attempt          workexecution.Attempt      `json:"attempt"`
		ForwardAttemptID string                     `json:"forward_attempt_id"`
		SourceResult     workexecution.Input        `json:"source_result"`
		Group            *GroupRestorationAuthority `json:"group,omitempty"`
		Report           RecoveryReport             `json:"report"`
	}{input.Input.Attempt, input.Forward.ID, *input.Forward.Result, input.Group, report})
	if err != nil {
		return workexecution.Checkpoint{}, invalidProgress()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(canonical)
	return workexecution.Checkpoint{AttemptID: input.Input.Attempt.ID, Sequence: report.Progress.Sequence, StageID: report.StageID,
		RecordedAt: report.Progress.UpdatedAt, Evidence: workexecution.Input{ID: input.Input.Attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}}, nil
}
