package resolverexecution

import (
	"bytes"
	"time"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Recovery always targets the exact captured predecessor. A missing predecessor
// means runtime absence and baseline host resolution, never a latest-state read.
type Recovery struct {
	Forward  workexecution.Attempt `json:"forward"`
	Failure  Failure               `json:"failure"`
	Evidence workexecution.Input   `json:"evidence"`
	Previous *RecoveryInterruption `json:"previous,omitempty"`
}

type RecoveryInterruption struct {
	Attempt  workexecution.Attempt `json:"attempt"`
	Report   RecoveryReport        `json:"report"`
	Evidence workexecution.Input   `json:"evidence"`
}

type RecoveryStatus string

const (
	RecoveryComplete    RecoveryStatus = "complete"
	RecoveryFailed      RecoveryStatus = "failed"
	RecoveryInterrupted RecoveryStatus = "interrupted"
	RecoveryTimedOut    RecoveryStatus = "timed_out"
)

type RecoveryReport struct {
	AttemptID             string                                  `json:"attempt_id"`
	ForwardID             string                                  `json:"forward_id"`
	Input                 workexecution.Input                     `json:"input"`
	Failure               workexecution.Input                     `json:"failure"`
	Status                RecoveryStatus                          `json:"status"`
	HelpersStopped        bool                                    `json:"helpers_stopped"`
	ConfigurationRestored bool                                    `json:"configuration_restored"`
	RuntimeRestored       bool                                    `json:"runtime_restored"`
	HostResolverRestored  bool                                    `json:"host_resolver_restored"`
	ServingProof          *agentpb.DNSResolverObservationEvidence `json:"serving_proof,omitempty"`
	ObservedAt            time.Time                               `json:"observed_at"`
}

// Validate checks captured recovery authority before assignment without
// fabricating an attempt or a replacement executor identity.
func (source Recovery) Validate(layout Layout) error {
	forward := source.Forward
	if layout.Validate(forward) != nil || forward.Direction != workexecution.Forward || forward.StartedAt == nil ||
		forward.FinishedAt == nil || forward.Result == nil || forward.Outcome == workexecution.Succeeded {
		return invalid("DNS resolver cleanup source has no actual failed forward execution")
	}
	_, evidence, err := EncodeFailure(forward, layout, source.Failure)
	if err != nil || evidence != source.Evidence || evidence != *forward.Result {
		return invalid("DNS resolver cleanup source differs from its retained failure")
	}
	if previous := source.Previous; previous != nil {
		prior := previous.Attempt
		if prior.Validate() != nil || prior.Direction != workexecution.Recovery || prior.FinishedAt == nil || prior.Result == nil ||
			prior.Outcome == workexecution.Succeeded || prior.Number <= forward.Number || prior.TaskID != forward.TaskID ||
			prior.WorkItemID != forward.WorkItemID || prior.Input != forward.Input || prior.Executor.HostID != forward.Executor.HostID ||
			previous.Report.Status == RecoveryComplete || previous.Report.validateEnvelope(prior, source) != nil {
			return invalid("DNS resolver cleanup source has no valid previous interruption")
		}
		id, err := derivedReferenceID(prior.ID, "cleanup")
		if err != nil {
			return err
		}
		_, evidence, err := captureEvidence(id, previous.Report)
		if err != nil || evidence != previous.Evidence || evidence != *prior.Result {
			return invalid("DNS resolver cleanup source changed its prior interruption receipt")
		}
	}
	return nil
}

func (source Recovery) ValidateFor(attempt workexecution.Attempt, layout Layout) error {
	forward := source.Forward
	if source.Validate(layout) != nil || layout.Validate(attempt) != nil || attempt.Direction != workexecution.Recovery ||
		forward.ID == attempt.ID || forward.TaskID != attempt.TaskID || forward.WorkItemID != attempt.WorkItemID ||
		forward.Input != attempt.Input || forward.Executor.HostID != attempt.Executor.HostID ||
		forward.Number >= attempt.Number || attempt.AssignedAt.Before(*forward.FinishedAt) {
		return invalid("DNS resolver cleanup has no actual failed forward attempt on this host")
	}
	if previous := source.Previous; previous != nil {
		prior := previous.Attempt
		if prior.Number+1 != attempt.Number || prior.ID == attempt.ID || attempt.AssignedAt.Before(*prior.FinishedAt) {
			return invalid("DNS resolver cleanup Retry has no exact previous interruption")
		}
	} else if attempt.Number != forward.Number+1 {
		return invalid("DNS resolver cleanup cannot omit an intervening attempt")
	}
	return nil
}

func (report RecoveryReport) validateEnvelope(attempt workexecution.Attempt, source Recovery) error {
	if report.AttemptID != attempt.ID || report.ForwardID != source.Forward.ID || report.Input != attempt.Input ||
		report.Failure != source.Evidence || !canonicalTime(report.ObservedAt) || report.ObservedAt.Before(attempt.AssignedAt) ||
		attempt.StartedAt != nil && report.ObservedAt.Before(*attempt.StartedAt) ||
		attempt.FinishedAt != nil && report.ObservedAt.After(*attempt.FinishedAt) {
		return invalid("DNS resolver cleanup report differs from its captured assignment and failure")
	}
	if report.Status == RecoveryComplete {
		if attempt.StartedAt == nil || !report.HelpersStopped || !report.ConfigurationRestored || !report.RuntimeRestored || !report.HostResolverRestored {
			return invalid("DNS resolver cleanup lacks termination or exact restoration proof")
		}
	} else if report.Status != RecoveryFailed && report.Status != RecoveryInterrupted && report.Status != RecoveryTimedOut ||
		report.HelpersStopped || report.ConfigurationRestored || report.RuntimeRestored || report.HostResolverRestored || report.ServingProof != nil ||
		report.Status == RecoveryTimedOut && report.ObservedAt.Before(attempt.Deadline) {
		return invalid("unsuccessful DNS resolver cleanup cannot claim effects settled")
	}
	return nil
}

// ValidateBoundary checks the report envelope against real retained recovery
// authority. Callers must separately validate serving proof against captured
// configuration before recording or settling successful restoration.
func (report RecoveryReport) ValidateBoundary(attempt workexecution.Attempt, source Recovery, layout Layout) error {
	if err := source.ValidateFor(attempt, layout); err != nil {
		return err
	}
	return report.validateEnvelope(attempt, source)
}

// RecoveryDigest hashes a validated receipt without retaining executable input.
func RecoveryDigest(attemptID string, report RecoveryReport) (workexecution.Input, error) {
	if report.AttemptID != attemptID {
		return workexecution.Input{}, invalid("DNS resolver cleanup digest names a different attempt")
	}
	id, err := derivedReferenceID(attemptID, "cleanup")
	if err != nil {
		return workexecution.Input{}, err
	}
	_, evidence, err := captureEvidence(id, report)
	return evidence, err
}

func EncodeRecoveryReport(attempt workexecution.Attempt, input Input, source Recovery, report RecoveryReport) ([]byte, workexecution.Input, error) {
	layout, err := CaptureLayout(attempt, input)
	if err != nil || source.ValidateFor(attempt, layout) != nil || report.validateEnvelope(attempt, source) != nil {
		return nil, workexecution.Input{}, invalid("DNS resolver cleanup report has no matching captured authority")
	}
	if report.Status == RecoveryComplete {
		if !input.PreviousEnabled && report.ServingProof != nil || input.PreviousEnabled &&
			(ValidateServingProof(*input.Previous, report.ServingProof) != nil || report.ServingProof.ObservedAt.AsTime().Before(*attempt.StartedAt) ||
				report.ServingProof.ObservedAt.AsTime().After(report.ObservedAt)) {
			return nil, workexecution.Input{}, invalid("DNS resolver cleanup did not prove its exact previous serving configuration")
		}
	}
	id, err := derivedReferenceID(attempt.ID, "cleanup")
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	return captureEvidence(id, report)
}

func DecodeRecoveryReport(attempt workexecution.Attempt, input Input, source Recovery, raw []byte) (RecoveryReport, workexecution.Input, error) {
	if len(raw) == 0 || len(raw) > MaximumReportBytes {
		return RecoveryReport{}, workexecution.Input{}, invalid("DNS resolver cleanup report is missing or oversized")
	}
	report, err := jcs.Decode[RecoveryReport](raw)
	if err != nil {
		return RecoveryReport{}, workexecution.Input{}, invalid("DNS resolver cleanup report cannot be decoded")
	}
	canonical, evidence, err := EncodeRecoveryReport(attempt, input, source, report)
	if err != nil || !bytes.Equal(canonical, raw) {
		return RecoveryReport{}, workexecution.Input{}, invalid("DNS resolver cleanup report is not canonical")
	}
	return report, evidence, nil
}
