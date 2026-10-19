package environmentexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Result records an observation, not a prediction from a helper exit code.
// Failure remains separate from cleanup even when the directory is now ready.
type Result struct {
	AttemptID      string    `json:"attempt_id"`
	DirectoryReady bool      `json:"directory_ready"`
	HelpersAbsent  bool      `json:"helpers_absent"`
	Failure        string    `json:"failure,omitempty"`
	CleanupFailure string    `json:"cleanup_failure,omitempty"`
	ObservedAt     time.Time `json:"observed_at"`
}

func (result Result) Validate(attempt workexecution.Attempt) error {
	if attempt.Validate() != nil || result.AttemptID != attempt.ID || result.ObservedAt.IsZero() ||
		result.ObservedAt.Location() != time.UTC || result.ObservedAt.Before(attempt.AssignedAt) ||
		len(
			result.Failure,
		) > 512 || len(result.CleanupFailure) > 512 || !utf8.ValidString(result.Failure) || !utf8.ValidString(result.CleanupFailure) ||
		!result.HelpersAbsent && result.CleanupFailure == "" || result.HelpersAbsent && result.CleanupFailure != "" ||
		attempt.Direction == workexecution.Forward && !result.DirectoryReady && result.Failure == "" {
		return invalid("environment provisioning result is invalid")
	}
	return nil
}

func (result Result) Reference(attempt workexecution.Attempt) (workexecution.Input, error) {
	if err := result.Validate(attempt); err != nil {
		return workexecution.Input{}, err
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, attempt.AssignedAt, attempt.ID, "environment-provision-result"),
		SHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func (result Result) Completion(attempt workexecution.Attempt) (workexecution.Completion, error) {
	ref, err := result.Reference(attempt)
	if err != nil {
		return workexecution.Completion{}, err
	}
	outcome := workexecution.Failed
	if result.HelpersAbsent &&
		(attempt.Direction == workexecution.Recovery || result.DirectoryReady && result.Failure == "") {
		outcome = workexecution.Succeeded
	}
	return workexecution.Completion{Outcome: outcome, Evidence: ref, EffectsSettled: result.HelpersAbsent}, nil
}

func DecodeResult(attempt workexecution.Attempt, message *agentpb.EnvironmentProvisionResultRequest) (Result, error) {
	if message == nil || executionplan.RejectUnknown(message) != nil || message.ObservedAt == nil ||
		message.ObservedAt.CheckValid() != nil {
		return Result{}, invalid("environment provisioning report is invalid")
	}
	identity, err := workexecution.DecodeAgentAttempt(message.Attempt, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(identity, attempt) {
		return Result{}, invalid("environment provisioning report belongs to another attempt")
	}
	result := Result{
		AttemptID:      attempt.ID,
		DirectoryReady: message.DirectoryReady,
		HelpersAbsent:  message.HelpersAbsent,
		Failure:        message.Failure,
		CleanupFailure: message.CleanupFailure,
		ObservedAt:     message.ObservedAt.AsTime().UTC(),
	}
	return result, result.Validate(attempt)
}

func EncodeResult(attempt workexecution.Attempt, result Result) (*agentpb.EnvironmentProvisionResultRequest, error) {
	if err := result.Validate(attempt); err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.EnvironmentProvisionResultRequest{Attempt: identity, DirectoryReady: result.DirectoryReady,
		HelpersAbsent: result.HelpersAbsent, Failure: result.Failure, CleanupFailure: result.CleanupFailure, ObservedAt: timestamppb.New(result.ObservedAt)}, nil
}

func DecodeResultAck(
	attempt workexecution.Attempt,
	result Result,
	ack *agentpb.EnvironmentProvisionResultAck,
) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.FinishedAt == nil ||
		ack.FinishedAt.CheckValid() != nil {
		return workexecution.Attempt{}, invalid("environment provisioning result receipt is invalid")
	}
	identity, err := workexecution.DecodeAgentAttempt(ack.Attempt, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(identity, attempt) {
		return workexecution.Attempt{}, invalid("environment provisioning receipt belongs to another attempt")
	}
	expected, err := result.Reference(attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	evidence, err := DecodeReference(ack.Evidence)
	if err != nil || evidence != expected {
		return workexecution.Attempt{}, invalid("environment provisioning receipt acknowledges a different result")
	}
	terminal := attempt
	finished := ack.FinishedAt.AsTime().UTC()
	terminal.FinishedAt, terminal.Result, terminal.Outcome = &finished, &evidence, workexecution.Outcome(ack.Outcome)
	if ack.StartedAt != nil {
		if ack.StartedAt.CheckValid() != nil {
			return workexecution.Attempt{}, invalid("environment provisioning start receipt is invalid")
		}
		started := ack.StartedAt.AsTime().UTC()
		if attempt.StartedAt == nil || !started.Equal(*attempt.StartedAt) {
			return workexecution.Attempt{}, invalid("environment provisioning start receipt changed")
		}
		terminal.StartedAt = &started
	} else if attempt.StartedAt != nil {
		return workexecution.Attempt{}, invalid("environment provisioning start receipt is missing")
	}
	if finished.Before(result.ObservedAt) {
		return workexecution.Attempt{}, invalid("environment provisioning receipt predates observation")
	}
	expectedCompletion, err := result.Completion(attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if terminal.Outcome != expectedCompletion.Outcome && (attempt.Direction != workexecution.Forward ||
		terminal.Outcome != workexecution.Cancelled && terminal.Outcome != workexecution.Superseded) {
		return workexecution.Attempt{}, invalid("environment provisioning receipt changes the reported outcome")
	}
	if err := terminal.Validate(); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
