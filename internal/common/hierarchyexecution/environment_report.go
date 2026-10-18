package hierarchyexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

const MaximumEnvironmentReportBytes = 4096

// EnvironmentReport carries bounded operation evidence, never Compose content,
// configuration files, or secret values. Its sequence is the attempt sequence.
type EnvironmentReport struct {
	Progress EnvironmentProgress `json:"progress"`
}

func (report EnvironmentReport) Checkpoint(input EnvironmentInput) (workexecution.Checkpoint, error) {
	if report.Progress.Validate(input) != nil || report.Progress.Sequence == 0 {
		return workexecution.Checkpoint{}, invalidEnvironmentProgress()
	}
	at, err := ids.Timestamp(ids.KindWorkItem, input.Attempt.WorkItemID)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	phase := RemoveRuntime
	if report.Progress.Sequence > 2 {
		phase = RemoveDirectory
	}
	stage := ids.DeriveAt(ids.KindStep, at, input.Attempt.WorkItemID, "environment-removal/"+string(phase))
	// Settlement is mutable execution state, not part of report identity. A
	// report acknowledged before completion must replay unchanged afterward.
	assignment := input.Attempt
	assignment.FinishedAt, assignment.Result, assignment.Outcome = nil, nil, ""
	raw, err := json.Marshal(struct {
		Attempt workexecution.Attempt `json:"attempt"`
		Report  EnvironmentReport     `json:"report"`
	}{assignment, report})
	if err != nil {
		return workexecution.Checkpoint{}, invalidEnvironmentProgress()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Checkpoint{}, err
	}
	digest := sha256.Sum256(canonical)
	return workexecution.Checkpoint{AttemptID: input.Attempt.ID, Sequence: report.Progress.Sequence, StageID: stage,
		RecordedAt: report.Progress.UpdatedAt, Evidence: workexecution.Input{ID: input.Attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}}, nil
}

func EncodeEnvironmentReport(input EnvironmentInput, report EnvironmentReport) ([]byte, error) {
	if _, err := report.Checkpoint(input); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return nil, invalidEnvironmentProgress()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaximumEnvironmentReportBytes {
		return nil, invalidEnvironmentProgress()
	}
	return canonical, nil
}

func DecodeEnvironmentReport(input EnvironmentInput, raw []byte) (EnvironmentReport, error) {
	if len(raw) == 0 || len(raw) > MaximumEnvironmentReportBytes {
		return EnvironmentReport{}, invalidEnvironmentProgress()
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var report EnvironmentReport
	if err := decoder.Decode(&report); err != nil {
		return EnvironmentReport{}, invalidEnvironmentProgress()
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return EnvironmentReport{}, invalidEnvironmentProgress()
	}
	canonical, err := EncodeEnvironmentReport(input, report)
	if err != nil || !bytes.Equal(canonical, raw) {
		return EnvironmentReport{}, invalidEnvironmentProgress()
	}
	return report, nil
}
