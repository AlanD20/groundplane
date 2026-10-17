package workexecution

import (
	"time"
	"unicode/utf8"
)

// Stage is safe operator-facing evidence, never a command payload or recovery
// grant. Producers must omit secrets and raw provider output. Key is stable
// within the captured operation, independent of execution order or retry count.
type Stage struct {
	Key         string     `json:"key"`
	Name        string     `json:"name"`
	State       StageState `json:"state"`
	Expected    string     `json:"expected"`
	Observed    string     `json:"observed,omitempty"`
	FailureCode string     `json:"failure_code,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

type StageState string

const (
	StageWaiting   StageState = "waiting"
	StageRunning   StageState = "running"
	StageSucceeded StageState = "succeeded"
	StageFailed    StageState = "failed"
	StageSkipped   StageState = "skipped"
	StageUnknown   StageState = "unknown"
)

func (stage Stage) Validate() error {
	if len(stage.Key) < 1 || len(stage.Key) > 64 || len(stage.Name) < 1 || len(stage.Name) > 160 ||
		len(stage.Expected) < 1 || len(stage.Expected) > 1024 || len(stage.Observed) > 2048 || len(stage.FailureCode) > 128 ||
		!utf8.ValidString(stage.Name) || !utf8.ValidString(stage.Expected) || !utf8.ValidString(stage.Observed) || !utf8.ValidString(stage.FailureCode) {
		return invalid("execution stage text exceeds its diagnostic bounds")
	}
	for _, ch := range stage.Key {
		if !(ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			return invalid("execution stage key is invalid")
		}
	}
	if stage.StartedAt != nil && !validTime(*stage.StartedAt) || stage.FinishedAt != nil && !validTime(*stage.FinishedAt) ||
		stage.StartedAt != nil && stage.FinishedAt != nil && stage.FinishedAt.Before(*stage.StartedAt) {
		return invalid("execution stage timing is invalid")
	}
	switch stage.State {
	case StageWaiting:
		if stage.StartedAt != nil || stage.FinishedAt != nil {
			return invalid("waiting stage cannot have execution timestamps")
		}
	case StageRunning:
		if stage.StartedAt == nil || stage.FinishedAt != nil {
			return invalid("running stage requires its start time")
		}
	case StageSucceeded, StageFailed:
		if stage.StartedAt == nil || stage.FinishedAt == nil {
			return invalid("completed stage requires execution timing")
		}
	case StageSkipped:
		if stage.StartedAt != nil || stage.FinishedAt == nil || stage.Observed == "" {
			return invalid("skipped stage requires a recorded reason and time")
		}
	case StageUnknown:
		if stage.StartedAt == nil || stage.FinishedAt != nil || stage.Observed == "" {
			return invalid("unknown stage requires its unresolved observation")
		}
	default:
		return invalid("execution stage state is invalid")
	}
	if stage.FailureCode != "" && stage.State != StageFailed && stage.State != StageUnknown {
		return invalid("stage failure code requires a failed or unknown outcome")
	}
	return nil
}

// AdvanceStage updates one attempt's observation without rewriting completed
// stages. A new attempt gets a separate diagnostic record.
func AdvanceStage(previous, next Stage) error {
	if err := previous.Validate(); err != nil {
		return err
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if previous.Key != next.Key || previous.Name != next.Name || previous.Expected != next.Expected {
		return conflict("stage update changed its captured identity or expectation")
	}
	if previous.FinishedAt != nil {
		return conflict("completed stage cannot be rewritten")
	}
	if previous.StartedAt != nil && (next.StartedAt == nil || !next.StartedAt.Equal(*previous.StartedAt)) {
		return conflict("stage update changed its start time")
	}
	if previous.State != StageWaiting && (next.State == StageWaiting || next.State == StageSkipped) {
		return conflict("started stage cannot return to waiting or skipped")
	}
	return nil
}
