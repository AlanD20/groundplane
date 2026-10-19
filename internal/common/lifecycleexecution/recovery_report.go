package lifecycleexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RecoveryReport observes the interrupted stage only after the previous helper
// and any recorded hook exec have stopped. A missing receipt means unresolved,
// not permission to replay. The original forward failure remains unchanged.
type RecoveryReport struct {
	FailureReason    FailureReason       `json:"failure_reason,omitempty"`
	AttemptID        string              `json:"attempt_id"`
	ForwardID        string              `json:"forward_id"`
	Failure          workexecution.Input `json:"failure"`
	ExecutionStopped bool                `json:"execution_stopped"`
	ObservedAt       time.Time           `json:"observed_at"`
	Receipt          *Receipt            `json:"receipt,omitempty"`
}

func (report RecoveryReport) Validate(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	source Recovery,
) error {
	if err := source.ValidateFor(attempt, input); err != nil {
		return err
	}
	stages, err := Stages(input)
	if err != nil {
		return err
	}
	return report.ValidateStages(attempt, stages, source)
}

func (report RecoveryReport) ValidateStages(attempt workexecution.Attempt, stages []Stage, source Recovery) error {
	if source.ValidateForStages(attempt, stages) != nil || attempt.StartedAt == nil || report.AttemptID != attempt.ID ||
		report.ForwardID != source.Forward.ID || report.Failure != source.Evidence ||
		source.Progress == nil || source.Progress.BeganAt == nil || report.ObservedAt.IsZero() || report.ObservedAt.Location() != time.UTC ||
		report.ObservedAt.Before(*attempt.StartedAt) || report.ObservedAt.Before(source.Failure.RecordedAt) {
		return invalid()
	}
	if !report.ExecutionStopped {
		if report.Receipt != nil || !report.FailureReason.Valid() ||
			report.FailureReason == DeadlineExpired && report.ObservedAt.Before(attempt.Deadline) {
			return invalid()
		}
		return nil
	}
	if report.FailureReason != "" {
		return invalid()
	}
	if report.Receipt == nil {
		return nil
	}
	if len(source.Progress.Completed) >= len(stages) {
		return invalid()
	}
	stage := stages[len(source.Progress.Completed)]
	receipt := report.Receipt
	if receipt.StageID != stage.ID || receipt.Member != stage.Member || receipt.State != stage.Expected ||
		receipt.Evidence.Validate() != nil || receipt.Evidence.ID != attempt.Input.ID ||
		receipt.ObservedAt.Location() != time.UTC || receipt.ObservedAt.Before(*source.Progress.BeganAt) || receipt.ObservedAt.After(report.ObservedAt) {
		return invalid()
	}
	// Hook success is retained from its recorded exec; container observations
	// instead must be fresh, after this recovery attempt obtained its Start.
	if stage.Expected != HookSucceeded && receipt.ObservedAt.Before(*attempt.StartedAt) {
		return invalid()
	}
	return nil
}

func (report RecoveryReport) BoundaryCompleted() bool {
	return report.ExecutionStopped && report.Receipt != nil
}

func EncodeRecoveryReport(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	source Recovery,
	report RecoveryReport,
) ([]byte, workexecution.Input, error) {
	if err := report.Validate(attempt, input, source); err != nil {
		return nil, workexecution.Input{}, err
	}
	stages, err := Stages(input)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	return EncodeRecoveryReportStages(attempt, stages, source, report)
}

// EncodeRecoveryReportStages validates journal metadata without persisting the
// full Compose artifact or any private hook inputs on the Agent.
func EncodeRecoveryReportStages(
	attempt workexecution.Attempt,
	stages []Stage,
	source Recovery,
	report RecoveryReport,
) ([]byte, workexecution.Input, error) {
	if err := report.ValidateStages(attempt, stages, source); err != nil {
		return nil, workexecution.Input{}, err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, workexecution.Input{}, invalid()
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil || len(raw) > MaximumProgressBytes {
		return nil, workexecution.Input{}, invalid()
	}
	at, err := ids.Timestamp(ids.KindAttempt, attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return raw, workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, at, attempt.ID, "service-lifecycle/recovery"),
		SHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func DecodeRecoveryReport(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	source Recovery,
	raw []byte,
) (RecoveryReport, workexecution.Input, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return RecoveryReport{}, workexecution.Input{}, invalid()
	}
	report, err := jcs.Decode[RecoveryReport](raw)
	if err != nil {
		return RecoveryReport{}, workexecution.Input{}, invalid()
	}
	expected, evidence, err := EncodeRecoveryReport(attempt, input, source, report)
	if err != nil || !bytes.Equal(raw, expected) {
		return RecoveryReport{}, workexecution.Input{}, invalid()
	}
	return report, evidence, nil
}
