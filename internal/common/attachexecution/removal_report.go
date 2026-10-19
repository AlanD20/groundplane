package attachexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

const MaximumRemovalReportBytes = 4096
const MaximumRemovalReports = 2 * (MaximumGrants + 2)

type RemovalReport struct {
	Progress RemovalProgress `json:"progress"`
}

func (report RemovalReport) Checkpoint(input RemovalInput) (workexecution.Checkpoint, error) {
	authority, err := input.Authority()
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	return report.CheckpointAuthority(input.Attempt, authority)
}

func (report RemovalReport) CheckpointAuthority(
	attempt workexecution.Attempt,
	authority RemovalAuthority,
) (workexecution.Checkpoint, error) {
	if report.Progress.ValidateAuthority(attempt, authority) != nil || report.Progress.Sequence == 0 {
		return workexecution.Checkpoint{}, invalid("Attach removal report has no acknowledged command")
	}
	at, err := ids.Timestamp(ids.KindWorkItem, attempt.WorkItemID)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	ordinal := report.Progress.Completed
	if !report.Progress.InFlight {
		ordinal--
	}
	stage := ids.DeriveAt(
		ids.KindStep,
		at,
		attempt.WorkItemID,
		"attach-removal/"+strconv.FormatUint(uint64(ordinal), 10),
	)
	assignment := attempt
	assignment.FinishedAt, assignment.Result, assignment.Outcome = nil, nil, ""
	raw, err := json.Marshal(struct {
		Attempt   workexecution.Attempt `json:"attempt"`
		Authority RemovalAuthority      `json:"authority"`
		Report    RemovalReport         `json:"report"`
	}{assignment, authority, report})
	if err != nil {
		return workexecution.Checkpoint{}, invalid("Attach removal report cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(canonical)
	return workexecution.Checkpoint{AttemptID: attempt.ID, Sequence: report.Progress.Sequence, StageID: stage,
		RecordedAt: report.Progress.UpdatedAt, Evidence: workexecution.Input{ID: attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}}, nil
}

func EncodeRemovalReport(input RemovalInput, report RemovalReport) ([]byte, error) {
	authority, err := input.Authority()
	if err != nil {
		return nil, err
	}
	return EncodeRemovalReportAuthority(input.Attempt, authority, report)
}

func EncodeRemovalReportAuthority(
	attempt workexecution.Attempt,
	authority RemovalAuthority,
	report RemovalReport,
) ([]byte, error) {
	if _, err := report.CheckpointAuthority(attempt, authority); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, invalid("Attach removal report cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaximumRemovalReportBytes {
		return nil, invalid("Attach removal report exceeds its bound")
	}
	return canonical, nil
}

func DecodeRemovalReport(input RemovalInput, raw []byte) (RemovalReport, error) {
	authority, err := input.Authority()
	if err != nil {
		return RemovalReport{}, err
	}
	return DecodeRemovalReportAuthority(input.Attempt, authority, raw)
}

func DecodeRemovalReportAuthority(
	attempt workexecution.Attempt,
	authority RemovalAuthority,
	raw []byte,
) (RemovalReport, error) {
	if len(raw) == 0 || len(raw) > MaximumRemovalReportBytes {
		return RemovalReport{}, invalid("Attach removal report is missing or oversized")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var report RemovalReport
	if err := decoder.Decode(&report); err != nil {
		return RemovalReport{}, invalid("Attach removal report is malformed")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return RemovalReport{}, invalid("Attach removal report has trailing content")
	}
	canonical, err := EncodeRemovalReportAuthority(attempt, authority, report)
	if err != nil || !bytes.Equal(canonical, raw) {
		return RemovalReport{}, invalid("Attach removal report is not canonical")
	}
	return report, nil
}

// ValidateRemovalTransition accepts exactly one acknowledged command boundary.
// It cannot skip work, repeat an ambiguous command or change its saved result.
func ValidateRemovalTransition(input RemovalInput, previous *RemovalProgress, next RemovalProgress) error {
	if next.Validate(input) != nil {
		return invalid("Attach removal checkpoint is invalid")
	}
	var prior RemovalProgress
	var err error
	if previous == nil {
		prior, err = NewRemovalProgress(input, *input.Attempt.StartedAt)
	} else {
		prior = *previous
		err = prior.Validate(input)
	}
	if err != nil {
		return err
	}
	var expected RemovalProgress
	if next.InFlight {
		expected, err = prior.Begin(input, next.UpdatedAt)
	} else {
		if next.LastResult == nil {
			return invalid("Attach removal checkpoint has no command result")
		}
		expected, err = prior.Confirm(input, *next.LastResult, next.UpdatedAt)
	}
	if err != nil {
		return err
	}
	want, err := EncodeRemovalReport(input, RemovalReport{Progress: expected})
	if err != nil {
		return err
	}
	got, err := EncodeRemovalReport(input, RemovalReport{Progress: next})
	if err != nil || !bytes.Equal(want, got) {
		return invalid("Attach removal checkpoint changed previously acknowledged evidence")
	}
	return nil
}
