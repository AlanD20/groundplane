package componentexecution

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

// RecoveryReport proves restoration of the captured predecessor after joining
// the failed forward helper. It does not turn a failed Apply into success.
// FilesRestored covers only input.Files: restore their captured prior contents
// or remove files that were absent before this operation. Application data is
// not part of Component compensation.
type RecoveryReport struct {
	AttemptID           string              `json:"attempt_id"`
	ForwardID           string              `json:"forward_id"`
	Failure             workexecution.Input `json:"failure"`
	ComponentID         string              `json:"component_id"`
	Input               workexecution.Input `json:"input"`
	HelperStopped       bool                `json:"helper_stopped"`
	FilesRestored       bool                `json:"files_restored"`
	RuntimeAbsent       bool                `json:"runtime_absent"`
	RuntimeReady        bool                `json:"runtime_ready"`
	RuntimeSHA256       string              `json:"runtime_sha256,omitempty"`
	ConfigurationSHA256 string              `json:"configuration_sha256,omitempty"`
	ObservedAt          time.Time           `json:"observed_at"`
}

func (report RecoveryReport) Validate(attempt workexecution.Attempt, source Recovery,
	input *agentpb.ComponentWorkInput, boundary Progress,
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
		attempt.StartedAt == nil ||
		report.AttemptID != attempt.ID ||
		report.ForwardID != source.Forward.ID ||
		report.Failure != source.Evidence ||
		report.ComponentID != restoration.Layout.ComponentID ||
		report.Input != attempt.Input ||
		!report.HelperStopped ||
		!report.FilesRestored ||
		source.Failure.Checkpoint == nil ||
		boundary.ValidateLayout(source.Forward, restoration.Layout) != nil ||
		report.ObservedAt.IsZero() ||
		report.ObservedAt.Location() != time.UTC ||
		report.ObservedAt.Before(*attempt.StartedAt) ||
		report.ObservedAt.Before(source.Failure.RecordedAt) {
		return invalidProgress()
	}
	checkpoint, err := boundary.LayoutCheckpoint(source.Forward, restoration.Layout)
	if err != nil || checkpoint != *source.Failure.Checkpoint {
		return invalidProgress()
	}
	if restoration.Absent {
		if !report.RuntimeAbsent || report.RuntimeReady || report.RuntimeSHA256 != "" {
			return invalidProgress()
		}
	} else {
		if report.RuntimeAbsent || !report.RuntimeReady || report.RuntimeSHA256 != restoration.RuntimeSHA256 {
			return invalidProgress()
		}
	}
	if report.ConfigurationSHA256 != restoration.ConfigurationSHA256 {
		return invalidProgress()
	}
	return nil
}

func EncodeRecoveryReport(attempt workexecution.Attempt, source Recovery, input *agentpb.ComponentWorkInput,
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
		return nil, workexecution.Input{}, invalidProgress()
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	if len(raw) > MaximumProgressBytes {
		return nil, workexecution.Input{}, invalidProgress()
	}
	at, err := ids.Timestamp(ids.KindAttempt, attempt.ID)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	digest := sha256.Sum256(raw)
	return raw, workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, at, attempt.ID, "component/recovery"),
		SHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func DecodeRecoveryReport(attempt workexecution.Attempt, source Recovery, input *agentpb.ComponentWorkInput,
	boundary Progress, raw []byte,
) (RecoveryReport, workexecution.Input, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return RecoveryReport{}, workexecution.Input{}, invalidProgress()
	}
	report, err := jcs.Decode[RecoveryReport](raw)
	if err != nil {
		return RecoveryReport{}, workexecution.Input{}, invalidProgress()
	}
	canonical, evidence, err := EncodeRecoveryReport(attempt, source, input, boundary, report)
	if err != nil || !bytes.Equal(canonical, raw) {
		return RecoveryReport{}, workexecution.Input{}, invalidProgress()
	}
	return report, evidence, nil
}
