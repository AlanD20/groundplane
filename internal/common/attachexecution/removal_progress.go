package attachexecution

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// RemovalProgress records command boundaries, not command output. An in-flight
// command is ambiguous after interruption and cannot be automatically repeated.
// Custom Detach therefore has one acknowledged command; built-ins have at most
// MaximumGrants revocations followed by credential removal.
type RemovalProgress struct {
	AttemptID  string                `json:"attempt_id"`
	Sequence   uint64                `json:"sequence"`
	Completed  uint32                `json:"completed"`
	InFlight   bool                  `json:"in_flight"`
	BeganAt    *time.Time            `json:"began_at,omitempty"`
	UpdatedAt  time.Time             `json:"updated_at"`
	LastResult *RemovalCommandResult `json:"last_result,omitempty"`
}

type RemovalCommandResult struct {
	Ordinal    uint32          `json:"ordinal"`
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt time.Time       `json:"finished_at"`
	ExitCode   int32           `json:"exit_code"`
	Consumer   *ConsumerResult `json:"consumer,omitempty"`
}

// ConsumerResult proves endpoint detachment and identifies the captured
// candidate for atomic Controller settlement. It claims no host YAML file write.
type ConsumerResult struct {
	NetworkID       string    `json:"network_id"`
	ServiceID       string    `json:"service_id"`
	Detached        bool      `json:"detached"`
	CandidateSHA256 string    `json:"candidate_sha256"`
	ObservedAt      time.Time `json:"observed_at"`
}

func SameRemovalCommandResult(left, right RemovalCommandResult) bool {
	if left.Ordinal != right.Ordinal || left.ExitCode != right.ExitCode ||
		!left.StartedAt.Equal(right.StartedAt) || !left.FinishedAt.Equal(right.FinishedAt) ||
		(left.Consumer == nil) != (right.Consumer == nil) {
		return false
	}
	if left.Consumer == nil {
		return true
	}
	l, r := left.Consumer, right.Consumer
	return l.NetworkID == r.NetworkID && l.ServiceID == r.ServiceID && l.Detached == r.Detached &&
		l.CandidateSHA256 == r.CandidateSHA256 && l.ObservedAt.Equal(r.ObservedAt)
}

func (input RemovalInput) Validate() error {
	if input.Attempt.Validate() != nil || input.Attempt.Direction != workexecution.Forward ||
		input.Attempt.Executor.Executor != workexecution.Agent || ValidateRemoval(input.Removal) != nil {
		return invalid("Attach removal requires its captured Agent assignment")
	}
	return nil
}

func (input RemovalInput) CommandCount() uint32 {
	if input.Removal == nil {
		return 0
	}
	var count uint32
	if credential := input.Removal.GetCredential(); credential != nil {
		count = uint32(len(credential.Grants) + 1)
	} else if input.Removal.GetCustomDetach() != nil {
		count = 1
	}
	if input.Removal.Consumer != nil {
		count++
	}
	return count
}

// ConsumerCommandOrdinal places network/configuration changes after grant
// revocation and before the credential or Custom Detach command.
func (input RemovalInput) ConsumerCommandOrdinal() (uint32, bool) {
	if input.Removal == nil || input.Removal.Consumer == nil {
		return 0, false
	}
	if credential := input.Removal.GetCredential(); credential != nil {
		return uint32(len(credential.Grants)), true
	}
	return 0, true
}

func (input RemovalInput) CredentialCommandOrdinal(ordinal uint32) (uint32, error) {
	if ordinal >= input.CommandCount() {
		return 0, invalid("Attach command ordinal is outside captured work")
	}
	if consumer, present := input.ConsumerCommandOrdinal(); present {
		if ordinal == consumer {
			return 0, invalid("consumer transition is not a credential command")
		}
		if ordinal > consumer {
			ordinal--
		}
	}
	return ordinal, nil
}

func NewRemovalProgress(input RemovalInput, at time.Time) (RemovalProgress, error) {
	progress := RemovalProgress{AttemptID: input.Attempt.ID, UpdatedAt: at.UTC()}
	if !at.Before(input.Attempt.Deadline) {
		return RemovalProgress{}, invalid("Attach removal started after its deadline")
	}
	return progress, progress.Validate(input)
}

func (progress RemovalProgress) Validate(input RemovalInput) error {
	authority, err := input.Authority()
	if err != nil {
		return err
	}
	return progress.ValidateAuthority(input.Attempt, authority)
}

func (progress RemovalProgress) ValidateAuthority(attempt workexecution.Attempt, authority RemovalAuthority) error {
	count := authority.Commands
	if authority.Validate(attempt) != nil || attempt.StartedAt == nil ||
		progress.AttemptID != attempt.ID || progress.Completed > count || progress.Sequence != uint64(progress.Completed)*2+boolSequence(progress.InFlight) ||
		progress.InFlight && progress.Completed == count || progress.InFlight != (progress.BeganAt != nil) ||
		progress.UpdatedAt.IsZero() || progress.UpdatedAt.Before(*attempt.StartedAt) ||
		(progress.LastResult != nil) != (progress.Completed > 0) {
		return invalid("Attach removal progress has inconsistent command boundaries")
	}
	if progress.BeganAt != nil && (progress.BeganAt.Before(*attempt.StartedAt) || !progress.BeganAt.Before(attempt.Deadline) || progress.BeganAt.After(progress.UpdatedAt)) {
		return invalid("Attach removal command has an invalid start time")
	}
	if result := progress.LastResult; result != nil {
		if result.Ordinal+1 != progress.Completed || result.ExitCode != 0 || result.StartedAt.Before(*attempt.StartedAt) ||
			!result.StartedAt.Before(attempt.Deadline) || result.FinishedAt.Before(result.StartedAt) ||
			result.FinishedAt.After(progress.UpdatedAt) || progress.BeganAt != nil && result.FinishedAt.After(*progress.BeganAt) {
			return invalid("Attach removal progress lacks a successful command result")
		}
		isConsumer := authority.HasConsumer && result.Ordinal == authority.ConsumerOrdinal
		if isConsumer != (result.Consumer != nil) {
			return invalid("Attach command result has the wrong consumer evidence")
		}
		if consumer := result.Consumer; consumer != nil {
			if consumer.NetworkID != authority.ConsumerNetworkID || consumer.ServiceID != authority.ConsumerServiceID ||
				!consumer.Detached || consumer.CandidateSHA256 != authority.ConsumerCandidateSHA256 ||
				consumer.ObservedAt.Before(result.StartedAt) || consumer.ObservedAt.After(result.FinishedAt) {
				return invalid("Detach result does not prove its consumer network and captured candidate")
			}
		}
	}
	return nil
}

func (progress RemovalProgress) Complete(input RemovalInput) bool {
	return progress.Validate(input) == nil && !progress.InFlight && progress.Completed == input.CommandCount()
}

func (progress RemovalProgress) Begin(input RemovalInput, at time.Time) (RemovalProgress, error) {
	if progress.Validate(input) != nil || progress.InFlight || progress.Complete(input) || at.Before(progress.UpdatedAt) || !at.Before(input.Attempt.Deadline) {
		return RemovalProgress{}, invalid("Attach removal cannot begin another command")
	}
	next := progress
	began := at.UTC()
	next.Sequence++
	next.InFlight, next.BeganAt, next.UpdatedAt = true, &began, began
	return next, next.Validate(input)
}

func (progress RemovalProgress) Confirm(input RemovalInput, result RemovalCommandResult, at time.Time) (RemovalProgress, error) {
	authority, err := input.Authority()
	if err != nil {
		return RemovalProgress{}, err
	}
	return progress.ConfirmAuthority(input.Attempt, authority, result, at)
}

func (progress RemovalProgress) ConfirmAuthority(attempt workexecution.Attempt, authority RemovalAuthority, result RemovalCommandResult, at time.Time) (RemovalProgress, error) {
	if progress.ValidateAuthority(attempt, authority) != nil || !progress.InFlight || at.Before(progress.UpdatedAt) || result.Ordinal != progress.Completed ||
		!result.StartedAt.Equal(*progress.BeganAt) || result.FinishedAt.After(at) {
		return RemovalProgress{}, invalid("Attach removal result does not match its acknowledged command")
	}
	next := progress
	next.Sequence++
	next.Completed++
	next.InFlight, next.BeganAt, next.UpdatedAt = false, nil, at.UTC()
	next.LastResult = &result
	return next, next.ValidateAuthority(attempt, authority)
}

func boolSequence(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}
