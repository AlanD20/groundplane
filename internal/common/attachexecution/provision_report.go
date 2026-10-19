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

const MaximumProvisionReportBytes = 4096
const MaximumProvisionReports = 2 * (MaximumGrants + 2)

type ProvisionReport struct {
	Progress ProvisionProgress `json:"progress"`
}

func EncodeProvisionReport(input ProvisionInput, report ProvisionReport) ([]byte, error) {
	authority, err := input.Authority()
	if err != nil {
		return nil, err
	}
	return EncodeProvisionReportAuthority(input.Attempt, authority, report)
}

func DecodeProvisionReport(input ProvisionInput, raw []byte) (ProvisionReport, error) {
	authority, err := input.Authority()
	if err != nil {
		return ProvisionReport{}, err
	}
	return DecodeProvisionReportAuthority(input.Attempt, authority, raw)
}

func (report ProvisionReport) CheckpointAuthority(
	attempt workexecution.Attempt,
	authority ProvisionAuthority,
) (workexecution.Checkpoint, error) {
	if report.Progress.Validate(attempt, authority) != nil || report.Progress.Sequence == 0 {
		return workexecution.Checkpoint{}, invalid("Attach provisioning report has no acknowledged command")
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
		"attach-provisioning/"+strconv.FormatUint(uint64(ordinal), 10),
	)
	assignment := attempt
	assignment.FinishedAt, assignment.Result, assignment.Outcome = nil, nil, ""
	raw, err := json.Marshal(struct {
		Attempt   workexecution.Attempt `json:"attempt"`
		Authority ProvisionAuthority    `json:"authority"`
		Report    ProvisionReport       `json:"report"`
	}{assignment, authority, report})
	if err != nil {
		return workexecution.Checkpoint{}, invalid("Attach provisioning report cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(canonical)
	return workexecution.Checkpoint{AttemptID: attempt.ID, Sequence: report.Progress.Sequence, StageID: stage,
		RecordedAt: report.Progress.UpdatedAt, Evidence: workexecution.Input{ID: attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}}, nil
}

func EncodeProvisionReportAuthority(
	attempt workexecution.Attempt,
	authority ProvisionAuthority,
	report ProvisionReport,
) ([]byte, error) {
	if _, err := report.CheckpointAuthority(attempt, authority); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, invalid("Attach provisioning report cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaximumProvisionReportBytes {
		return nil, invalid("Attach provisioning report exceeds its bound")
	}
	return canonical, nil
}

func DecodeProvisionReportAuthority(
	attempt workexecution.Attempt,
	authority ProvisionAuthority,
	raw []byte,
) (ProvisionReport, error) {
	if len(raw) == 0 || len(raw) > MaximumProvisionReportBytes {
		return ProvisionReport{}, invalid("Attach provisioning report is missing or oversized")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var report ProvisionReport
	if err := decoder.Decode(&report); err != nil {
		return ProvisionReport{}, invalid("Attach provisioning report is malformed")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return ProvisionReport{}, invalid("Attach provisioning report has trailing content")
	}
	canonical, err := EncodeProvisionReportAuthority(attempt, authority, report)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ProvisionReport{}, invalid("Attach provisioning report is not canonical")
	}
	return report, nil
}

// ValidateProvisionTransition accepts exactly one acknowledged command boundary.
// It cannot skip work, repeat an ambiguous command or change its saved result.
func ValidateProvisionTransition(
	attempt workexecution.Attempt,
	authority ProvisionAuthority,
	previous *ProvisionProgress,
	next ProvisionProgress,
) error {
	if next.Validate(attempt, authority) != nil {
		return invalid("Attach provisioning checkpoint is invalid")
	}
	var prior ProvisionProgress
	var err error
	if previous == nil {
		prior, err = NewProvisionProgress(attempt, authority, *attempt.StartedAt)
	} else {
		prior = *previous
		err = prior.Validate(attempt, authority)
	}
	if err != nil {
		return err
	}
	var expected ProvisionProgress
	if next.InFlight {
		expected, err = prior.Begin(attempt, authority, next.UpdatedAt)
	} else {
		if next.LastResult == nil {
			return invalid("Attach provisioning checkpoint has no command result")
		}
		expected, err = prior.Confirm(attempt, authority, *next.LastResult, next.UpdatedAt)
	}
	if err != nil {
		return err
	}
	want, err := EncodeProvisionReportAuthority(attempt, authority, ProvisionReport{Progress: expected})
	if err != nil {
		return err
	}
	got, err := EncodeProvisionReportAuthority(attempt, authority, ProvisionReport{Progress: next})
	if err != nil || !bytes.Equal(want, got) {
		return invalid("Attach provisioning checkpoint changed previously acknowledged evidence")
	}
	return nil
}
