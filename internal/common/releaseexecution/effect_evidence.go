package releaseexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// EffectResult is a closed result set for Release host actions. Exactly one
// member is populated; the active cursor determines which member is legal.
type EffectResult struct {
	Configuration *ConfigurationProof `json:"configuration,omitempty"`
	Helper        []byte              `json:"helper,omitempty"`
	Readiness     *ReadinessProof     `json:"readiness,omitempty"`
	DrainedAt     *time.Time          `json:"drained_at,omitempty"`
}

// EffectEvidence retains bounded proof, not another copy of artifacts/secrets.
type EffectEvidence struct {
	AttemptID  string              `json:"attempt_id"`
	Input      workexecution.Input `json:"input"`
	Sequence   uint64              `json:"sequence"`
	StageID    string              `json:"stage_id"`
	RecordedAt time.Time           `json:"recorded_at"`
	Result     EffectResult        `json:"result"`
}

func EncodeEffectEvidence(input Input, report CheckpointReport, result EffectResult, at time.Time) ([]byte, workexecution.Input, error) {
	if report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil || result.validate(input, report.Progress, at) != nil ||
		at.IsZero() || at.Before(report.Progress.UpdatedAt) {
		return nil, workexecution.Input{}, invalidProgress()
	}
	stage, err := report.Progress.StageID(input)
	if err != nil || stage != report.StageID {
		return nil, workexecution.Input{}, invalidProgress()
	}
	record := EffectEvidence{AttemptID: input.Attempt.ID, Input: input.Attempt.Input, Sequence: report.Sequence, StageID: stage, RecordedAt: at.UTC(), Result: result}
	raw, err := json.Marshal(record)
	if err != nil || len(raw) > MaximumCheckpointBytes {
		return nil, workexecution.Input{}, invalidProgress()
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	reference, err := effectEvidenceReference(input, report, raw)
	return raw, reference, err
}

func DecodeEffectEvidence(input Input, report CheckpointReport, raw []byte, reference workexecution.Input) (EffectEvidence, error) {
	if len(raw) == 0 || len(raw) > MaximumCheckpointBytes {
		return EffectEvidence{}, invalidProgress()
	}
	record, err := jcs.Decode[EffectEvidence](raw)
	if err != nil || record.AttemptID != input.Attempt.ID || record.Input != input.Attempt.Input || record.Sequence != report.Sequence || record.StageID != report.StageID {
		return EffectEvidence{}, invalidProgress()
	}
	canonical, expected, err := EncodeEffectEvidence(input, report, record.Result, record.RecordedAt)
	if err != nil || !bytes.Equal(canonical, raw) || expected != reference {
		return EffectEvidence{}, invalidProgress()
	}
	return record, nil
}

func effectEvidenceReference(input Input, report CheckpointReport, raw []byte) (workexecution.Input, error) {
	at, err := ids.Timestamp(ids.KindAttempt, input.Attempt.ID)
	if err != nil {
		return workexecution.Input{}, err
	}
	id := ids.DeriveAt(ids.KindPlan, at, input.Attempt.ID, fmt.Sprintf("release/effect/%d/%s", report.Sequence, report.StageID))
	digest := sha256.Sum256(raw)
	return workexecution.Input{ID: id, SHA256: hex.EncodeToString(digest[:])}, nil
}

func HelperResult(response *agentpb.ReleaseHelperResponse) (EffectResult, error) {
	if response == nil {
		return EffectResult{}, invalidProgress()
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	return EffectResult{Helper: raw}, err
}
