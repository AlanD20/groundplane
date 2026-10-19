package entryexecution

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

// RecoveryReport accounts for only the captured file destinations and selected
// consumers. False flags retain unresolved effects; they never authorize replay.
type RecoveryReport struct {
	AttemptID                string              `json:"attempt_id"`
	ForwardID                string              `json:"forward_id"`
	Failure                  workexecution.Input `json:"failure"`
	EnvironmentID            string              `json:"environment_id"`
	Input                    workexecution.Input `json:"input"`
	EffectsStopped           bool                `json:"effects_stopped"`
	FilesRestored            bool                `json:"files_restored"`
	ConsumersRestored        bool                `json:"consumers_restored"`
	BaselineSHA256           string              `json:"baseline_sha256,omitempty"`
	PriorConfigurationSHA256 string              `json:"prior_configuration_sha256,omitempty"`
	ObservedAt               time.Time           `json:"observed_at"`
}

func (report RecoveryReport) Validate(attempt workexecution.Attempt, source Recovery,
	input *agentpb.EntryConfigurationWorkInput, boundary Progress,
) error {
	restoration, err := CaptureRestoration(attempt, input)
	if err != nil {
		return err
	}
	return report.ValidateRestoration(attempt, source, restoration, boundary)
}

func (report RecoveryReport) ValidateRestoration(
	attempt workexecution.Attempt,
	source Recovery,
	restoration Restoration,
	boundary Progress,
) error {
	if restoration.Validate(attempt) != nil || source.ValidateLayoutFor(attempt, restoration.Layout) != nil ||
		report.AttemptID != attempt.ID || report.ForwardID != source.Forward.ID || report.Failure != source.Evidence ||
		report.EnvironmentID != restoration.Layout.EnvironmentID || report.Input != attempt.Input ||
		report.ObservedAt.IsZero() || report.ObservedAt.Location() != time.UTC ||
		report.ObservedAt.Before(attempt.AssignedAt) || report.ObservedAt.Before(source.Failure.RecordedAt) {
		return invalidLayout()
	}
	if attempt.StartedAt == nil {
		if report.EffectsStopped || report.FilesRestored || report.ConsumersRestored {
			return invalidLayout()
		}
	} else if report.ObservedAt.Before(*attempt.StartedAt) {
		return invalidLayout()
	}
	checkpoint, err := boundary.LayoutCheckpoint(source.Forward, restoration.Layout)
	if err != nil || source.Failure.Checkpoint == nil || checkpoint != *source.Failure.Checkpoint {
		return invalidLayout()
	}
	if !report.EffectsStopped && (report.FilesRestored || report.ConsumersRestored) ||
		report.ConsumersRestored && (!report.FilesRestored || len(restoration.Layout.Consumers) == 0) {
		return invalidLayout()
	}
	baseline, configuration := "", ""
	if report.ConsumersRestored {
		baseline = restoration.BaselineSHA256
	}
	if report.FilesRestored {
		configuration = restoration.ConfigurationSHA256
	}
	if report.BaselineSHA256 != baseline || report.PriorConfigurationSHA256 != configuration {
		return invalidLayout()
	}
	return nil
}

func (report RecoveryReport) Settled(input *agentpb.EntryConfigurationWorkInput) bool {
	return input != nil && report.EffectsStopped && report.FilesRestored &&
		(len(input.RunningConsumerIds) == 0 || report.ConsumersRestored)
}

func RecoveryResultID(attemptID string) (string, error) {
	at, err := ids.Timestamp(ids.KindAttempt, attemptID)
	if err != nil {
		return "", err
	}
	return ids.DeriveAt(ids.KindPlan, at, attemptID, "entry/recovery-result"), nil
}

func EncodeRecoveryReport(attempt workexecution.Attempt, source Recovery, input *agentpb.EntryConfigurationWorkInput,
	boundary Progress, report RecoveryReport,
) ([]byte, workexecution.Input, error) {
	restoration, err := CaptureRestoration(attempt, input)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	return EncodeRestorationReport(attempt, source, restoration, boundary, report)
}

func EncodeRestorationReport(
	attempt workexecution.Attempt,
	source Recovery,
	restoration Restoration,
	boundary Progress,
	report RecoveryReport,
) ([]byte, workexecution.Input, error) {
	if err := report.ValidateRestoration(attempt, source, restoration, boundary); err != nil {
		return nil, workexecution.Input{}, err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, workexecution.Input{}, invalidLayout()
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil || len(raw) > MaximumRecoveryBytes {
		return nil, workexecution.Input{}, invalidLayout()
	}
	id, err := RecoveryResultID(attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return raw, workexecution.Input{ID: id, SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeRecoveryReport(attempt workexecution.Attempt, source Recovery, input *agentpb.EntryConfigurationWorkInput,
	boundary Progress, raw []byte, reference workexecution.Input,
) (RecoveryReport, error) {
	if len(raw) == 0 || len(raw) > MaximumRecoveryBytes {
		return RecoveryReport{}, invalidLayout()
	}
	report, err := jcs.Decode[RecoveryReport](raw)
	if err != nil {
		return RecoveryReport{}, invalidLayout()
	}
	expected, evidence, err := EncodeRecoveryReport(attempt, source, input, boundary, report)
	if err != nil || evidence != reference || !bytes.Equal(expected, raw) {
		return RecoveryReport{}, invalidLayout()
	}
	return report, nil
}
