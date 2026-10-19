package attachexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// ProvisionProgress retains command boundaries only. Custom hook facts travel
// separately for Controller encryption; this record never proves their storage
// and is not sufficient by itself to publish an Attach binding.
type ProvisionProgress struct {
	AttemptID  string                  `json:"attempt_id"`
	Sequence   uint64                  `json:"sequence"`
	Completed  uint32                  `json:"completed"`
	InFlight   bool                    `json:"in_flight"`
	BeganAt    *time.Time              `json:"began_at,omitempty"`
	UpdatedAt  time.Time               `json:"updated_at"`
	LastResult *ProvisionCommandResult `json:"last_result,omitempty"`
}

type ProvisionCommandResult struct {
	Ordinal    uint32                   `json:"ordinal"`
	StartedAt  time.Time                `json:"started_at"`
	FinishedAt time.Time                `json:"finished_at"`
	ExitCode   int32                    `json:"exit_code"`
	Consumer   *ProvisionConsumerResult `json:"consumer,omitempty"`
}

type ProvisionConsumerResult struct {
	NetworkID       string    `json:"network_id"`
	ServiceID       string    `json:"service_id"`
	CandidateSHA256 string    `json:"candidate_sha256"`
	ObservedAt      time.Time `json:"observed_at"`
	Connected       bool      `json:"connected"`
}

func SameProvisionCommandResult(left, right ProvisionCommandResult) bool {
	if (left.Consumer == nil) != (right.Consumer == nil) {
		return false
	}
	if left.Consumer != nil {
		a, b := left.Consumer, right.Consumer
		if a.NetworkID != b.NetworkID || a.ServiceID != b.ServiceID || a.CandidateSHA256 != b.CandidateSHA256 ||
			a.Connected != b.Connected ||
			!a.ObservedAt.Equal(b.ObservedAt) {
			return false
		}
	}
	return left.Ordinal == right.Ordinal && left.ExitCode == right.ExitCode &&
		left.StartedAt.Equal(right.StartedAt) && left.FinishedAt.Equal(right.FinishedAt)
}

func NewProvisionProgress(
	attempt workexecution.Attempt,
	authority ProvisionAuthority,
	at time.Time,
) (ProvisionProgress, error) {
	progress := ProvisionProgress{AttemptID: attempt.ID, UpdatedAt: at.UTC()}
	if !at.Before(attempt.Deadline) {
		return ProvisionProgress{}, invalid("Attach provisioning started after its deadline")
	}
	return progress, progress.Validate(attempt, authority)
}

func (progress ProvisionProgress) Validate(attempt workexecution.Attempt, authority ProvisionAuthority) error {
	if authority.Validate(attempt) != nil || attempt.StartedAt == nil || progress.AttemptID != attempt.ID ||
		progress.Completed > authority.Commands || progress.Sequence != uint64(progress.Completed)*2+boolSequence(progress.InFlight) ||
		progress.InFlight &&
			progress.Completed == authority.Commands || progress.InFlight != (progress.BeganAt != nil) ||
		progress.UpdatedAt.IsZero() || progress.UpdatedAt.Before(*attempt.StartedAt) ||
		(progress.LastResult != nil) != (progress.Completed > 0) {
		return invalid("Attach provisioning progress has inconsistent command boundaries")
	}
	if progress.BeganAt != nil && (progress.BeganAt.Before(*attempt.StartedAt) ||
		!progress.BeganAt.Before(attempt.Deadline) || progress.BeganAt.After(progress.UpdatedAt)) {
		return invalid("Attach provisioning command has an invalid start time")
	}
	if result := progress.LastResult; result != nil {
		if authority.IsConsumerCommand(result.Ordinal) != (result.Consumer != nil) {
			return invalid("Attach result has mismatched connection evidence")
		}
		if consumer := result.Consumer; consumer != nil {
			if !consumer.Connected || consumer.NetworkID != authority.ConsumerNetworkID ||
				consumer.ServiceID != authority.ConsumerServiceID ||
				consumer.CandidateSHA256 != authority.ConsumerCandidateSHA256 ||
				consumer.ObservedAt.IsZero() ||
				consumer.ObservedAt.Location() != time.UTC ||
				consumer.ObservedAt.Before(result.StartedAt) ||
				consumer.ObservedAt.After(result.FinishedAt) {
				return invalid("Attach result does not prove the captured consumer connection")
			}
		}
		if result.Ordinal+1 != progress.Completed || result.ExitCode != 0 ||
			result.StartedAt.Before(*attempt.StartedAt) ||
			!result.StartedAt.Before(attempt.Deadline) ||
			result.FinishedAt.Before(result.StartedAt) ||
			result.FinishedAt.After(progress.UpdatedAt) ||
			progress.BeganAt != nil && result.FinishedAt.After(*progress.BeganAt) {
			return invalid("Attach provisioning progress lacks a successful command result")
		}
	}
	return nil
}

func (progress ProvisionProgress) Complete(attempt workexecution.Attempt, authority ProvisionAuthority) bool {
	return progress.Validate(attempt, authority) == nil && !progress.InFlight &&
		progress.Completed == authority.Commands
}

// Begin refuses an in-flight command, including after reconnect. An uncertain
// external effect requires operation-owned inspection or explicit safe Retry;
// receiving the same assignment is not permission to execute it again.
func (progress ProvisionProgress) Begin(
	attempt workexecution.Attempt,
	authority ProvisionAuthority,
	at time.Time,
) (ProvisionProgress, error) {
	if progress.Validate(attempt, authority) != nil || progress.InFlight || progress.Complete(attempt, authority) ||
		at.Before(progress.UpdatedAt) || !at.Before(attempt.Deadline) {
		return ProvisionProgress{}, invalid("Attach provisioning cannot begin another command")
	}
	next := progress
	began := at.UTC()
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = true, &began, began
	return next, next.Validate(attempt, authority)
}

func (progress ProvisionProgress) Confirm(attempt workexecution.Attempt, authority ProvisionAuthority,
	result ProvisionCommandResult, at time.Time,
) (ProvisionProgress, error) {
	if progress.Validate(attempt, authority) != nil || !progress.InFlight || at.Before(progress.UpdatedAt) ||
		result.Ordinal != progress.Completed || !result.StartedAt.Equal(*progress.BeganAt) || result.FinishedAt.After(at) {
		return ProvisionProgress{}, invalid("Attach provisioning result does not match its acknowledged command")
	}
	next := progress
	next.Sequence++
	next.Completed++
	next.InFlight, next.BeganAt, next.UpdatedAt = false, nil, at.UTC()
	next.LastResult = &result
	return next, next.Validate(attempt, authority)
}
