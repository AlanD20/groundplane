package backingprovision

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type RecoveryStatus string

const (
	RecoveryComplete    RecoveryStatus = "complete"
	RecoveryFailed      RecoveryStatus = "failed"
	RecoveryInterrupted RecoveryStatus = "interrupted"
	RecoveryTimedOut    RecoveryStatus = "timed_out"
)

// RecoveryReport accounts for a first creation's captured absent predecessor.
// Only the exact runtime is removed. Directory contents, databases and managed
// Volume data are not deleted, restored or represented as rolled back.
type RecoveryReport struct {
	AttemptID     string              `json:"attempt_id"`
	ForwardID     string              `json:"forward_id"`
	Failure       workexecution.Input `json:"failure"`
	Input         workexecution.Input `json:"input"`
	ServiceID     string              `json:"service_id"`
	Status        RecoveryStatus      `json:"status"`
	HelperStopped bool                `json:"helper_stopped"`
	FilesStopped  bool                `json:"files_stopped"`
	RuntimeAbsent bool                `json:"runtime_absent"`
	ObservedAt    time.Time           `json:"observed_at"`
}

func (report RecoveryReport) Validate(attempt workexecution.Attempt, source Recovery, layout Layout) error {
	active := attempt
	active.FinishedAt, active.Outcome, active.Result = nil, "", nil
	if source.ValidateFor(active, layout) != nil || report.AttemptID != attempt.ID ||
		report.ForwardID != source.Forward.ID || report.Failure != source.Evidence || report.Input != attempt.Input ||
		report.ServiceID != layout.ServiceID || report.ObservedAt.IsZero() || report.ObservedAt.Location() != time.UTC ||
		report.ObservedAt.Before(attempt.AssignedAt) || attempt.StartedAt != nil && report.ObservedAt.Before(*attempt.StartedAt) ||
		report.ObservedAt.Before(source.Failure.RecordedAt) ||
		attempt.FinishedAt != nil && report.ObservedAt.After(*attempt.FinishedAt) {
		return invalid("backing creation cleanup report differs from its actual captured failure and assignment")
	}
	switch report.Status {
	case RecoveryComplete:
		if attempt.StartedAt == nil || !report.HelperStopped || !report.FilesStopped || !report.RuntimeAbsent {
			return invalid("backing creation cleanup is missing helper, file or runtime termination proof")
		}
	case RecoveryFailed, RecoveryInterrupted, RecoveryTimedOut:
		if report.HelperStopped || report.FilesStopped || report.RuntimeAbsent ||
			report.Status == RecoveryTimedOut && report.ObservedAt.Before(attempt.Deadline) {
			return invalid("unsuccessful backing creation cleanup cannot claim its effects settled")
		}
	default:
		return invalid("backing creation cleanup status is invalid")
	}
	return nil
}

func EncodeRecoveryReport(attempt workexecution.Attempt, source Recovery, layout Layout, report RecoveryReport) ([]byte, workexecution.Input, error) {
	if err := report.Validate(attempt, source, layout); err != nil {
		return nil, workexecution.Input{}, err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, workexecution.Input{}, invalid("backing creation cleanup report cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil || len(canonical) > MaximumProgressBytes {
		return nil, workexecution.Input{}, invalid("backing creation cleanup report exceeds its bounded format")
	}
	at, err := ids.Timestamp(ids.KindAttempt, attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(canonical)
	return canonical, workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, at, attempt.ID, "backing-creation/cleanup"), SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeRecoveryReport(attempt workexecution.Attempt, source Recovery, layout Layout, raw []byte) (RecoveryReport, workexecution.Input, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return RecoveryReport{}, workexecution.Input{}, invalid("backing creation cleanup report is missing or oversized")
	}
	report, err := jcs.Decode[RecoveryReport](raw)
	if err != nil {
		return RecoveryReport{}, workexecution.Input{}, invalid("backing creation cleanup report is malformed")
	}
	canonical, evidence, err := EncodeRecoveryReport(attempt, source, layout, report)
	if err != nil || !bytes.Equal(canonical, raw) {
		return RecoveryReport{}, workexecution.Input{}, invalid("backing creation cleanup report differs from its canonical evidence")
	}
	return report, evidence, nil
}

func DecodeRecoveryAck(attempt workexecution.Attempt, source Recovery, layout Layout, report RecoveryReport,
	ack *agentpb.BackingProvisionRecoveryAck,
) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil {
		return workexecution.Attempt{}, invalid("backing creation cleanup acknowledgement is invalid")
	}
	terminal, err := decodeTerminal(attempt, ack.Attempt, ack.Evidence, ack.Outcome, ack.StartedAt, ack.FinishedAt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	_, evidence, err := EncodeRecoveryReport(terminal, source, layout, report)
	outcome := workexecution.Failed
	if report.Status == RecoveryComplete {
		outcome = workexecution.Succeeded
	}
	if err != nil || evidence != *terminal.Result || terminal.Outcome != outcome {
		return workexecution.Attempt{}, invalid("backing creation cleanup acknowledgement differs from its saved proof")
	}
	return terminal, nil
}

// RuntimeCleanupObservation is only the helper's own runtime proof. The Agent
// separately joins its actual helper and file-writer termination before sending
// a complete RecoveryReport; the helper cannot assert those parent-side actions.
type RuntimeCleanupObservation struct {
	AttemptID     string              `json:"attempt_id"`
	ForwardID     string              `json:"forward_id"`
	Failure       workexecution.Input `json:"failure"`
	Input         workexecution.Input `json:"input"`
	ServiceID     string              `json:"service_id"`
	RuntimeAbsent bool                `json:"runtime_absent"`
	ObservedAt    time.Time           `json:"observed_at"`
}

func HelperRecovery(input HelperInput, observedAt time.Time) (*agentpb.BackingProvisionHelperResponse, error) {
	if input.Assignment.Recovery == nil || input.Progress != nil || input.Assignment.Attempt.StartedAt == nil ||
		input.Assignment.Recovery.ValidateFor(input.Assignment.Attempt, input.Layout) != nil ||
		observedAt.Location() != time.UTC || observedAt.Before(*input.Assignment.Attempt.StartedAt) {
		return nil, invalid("backing creation cleanup helper has no matching captured recovery")
	}
	report := RuntimeCleanupObservation{AttemptID: input.Assignment.Attempt.ID, ForwardID: input.Assignment.Recovery.Forward.ID,
		Failure: input.Assignment.Recovery.Evidence, Input: input.Assignment.Attempt.Input, ServiceID: input.Layout.ServiceID,
		RuntimeAbsent: true, ObservedAt: observedAt}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, err
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	return &agentpb.BackingProvisionHelperResponse{RecoveryReport: canonical}, nil
}

func ReadHelperRecovery(input HelperInput, response *agentpb.BackingProvisionHelperResponse) (RuntimeCleanupObservation, error) {
	if input.Assignment.Recovery == nil || response == nil || executionplan.RejectUnknown(response) != nil ||
		len(response.Observation) != 0 || len(response.RuntimeObservation) != 0 || len(response.RecoveryReport) == 0 || len(response.RecoveryReport) > MaximumProgressBytes {
		return RuntimeCleanupObservation{}, invalid("backing creation cleanup helper response is invalid")
	}
	report, err := jcs.Decode[RuntimeCleanupObservation](response.RecoveryReport)
	if err != nil {
		return RuntimeCleanupObservation{}, invalid("backing creation cleanup helper report is malformed")
	}
	expected, err := HelperRecovery(input, report.ObservedAt)
	if err != nil || !bytes.Equal(expected.RecoveryReport, response.RecoveryReport) {
		return RuntimeCleanupObservation{}, invalid("backing creation cleanup helper did not prove the captured runtime absent")
	}
	return report, nil
}
