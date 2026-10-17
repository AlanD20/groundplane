package volumeexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// RecoveryReport is a fresh host inspection after the exact forward helper has
// been stopped and joined. It cannot authorize replay of the interrupted effect.
// The original forward failure remains the action outcome.
type RecoveryReport struct {
	AttemptID     string              `json:"attempt_id"`
	ForwardID     string              `json:"forward_id"`
	Failure       workexecution.Input `json:"failure"`
	HelperStopped bool                `json:"helper_stopped"`
	Observation   Observation         `json:"observation"`
}

func (report RecoveryReport) Validate(attempt workexecution.Attempt, source Recovery, action agentpb.VolumeWorkAction, volumeID string, boundary Progress) error {
	if source.ValidateFor(attempt) != nil || attempt.StartedAt == nil || report.AttemptID != attempt.ID ||
		report.ForwardID != source.Forward.ID || report.Failure != source.Evidence || !report.HelperStopped ||
		source.Failure.Checkpoint == nil || boundary.Validate(source.Forward, action, volumeID) != nil || !boundary.InFlight {
		return invalidProgress()
	}
	checkpoint, err := boundary.Checkpoint(source.Forward, action, volumeID)
	if err != nil || checkpoint != *source.Failure.Checkpoint {
		return invalidProgress()
	}
	observed := report.Observation
	if observed.VolumeID != volumeID || observed.Input != attempt.Input || observed.At.IsZero() ||
		observed.At.Before(*attempt.StartedAt) || observed.At.Before(source.Failure.RecordedAt) ||
		observed.DirectoryReady && observed.DirectoryAbsent || observed.VolumeReady && observed.VolumeAbsent {
		return invalidProgress()
	}
	return nil
}

// BoundaryCompleted proves the interrupted stage, not successful completion of
// all later stages. Consumers must retain the captured continuation separately.
func (report RecoveryReport) BoundaryCompleted(boundary Progress) bool {
	return report.HelperStopped && report.Observation.proves(boundary.Stage)
}

func EncodeRecoveryReport(attempt workexecution.Attempt, source Recovery, action agentpb.VolumeWorkAction, volumeID string, boundary Progress, report RecoveryReport) ([]byte, workexecution.Input, error) {
	if err := report.Validate(attempt, source, action, volumeID, boundary); err != nil {
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
	return raw, workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, at, attempt.ID, "volume/recovery"), SHA256: hex.EncodeToString(digest[:])}, nil
}

func DecodeRecoveryReport(attempt workexecution.Attempt, source Recovery, action agentpb.VolumeWorkAction, volumeID string, boundary Progress, raw []byte) (RecoveryReport, workexecution.Input, error) {
	if len(raw) == 0 || len(raw) > MaximumProgressBytes {
		return RecoveryReport{}, workexecution.Input{}, invalidProgress()
	}
	report, err := jcs.Decode[RecoveryReport](raw)
	if err != nil {
		return RecoveryReport{}, workexecution.Input{}, invalidProgress()
	}
	canonical, evidence, err := EncodeRecoveryReport(attempt, source, action, volumeID, boundary, report)
	if err != nil || !bytes.Equal(canonical, raw) {
		return RecoveryReport{}, workexecution.Input{}, invalidProgress()
	}
	return report, evidence, nil
}
