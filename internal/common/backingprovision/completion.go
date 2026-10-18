package backingprovision

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// CompletionEvidence binds the final acknowledged stage and real container
// observation to the exact attempt. The operation owner additionally proves
// this is its complete procedure and matches the captured runtime before write.
// Terminal fields are excluded because this same digest becomes Attempt.Result.
func CompletionEvidence(attempt workexecution.Attempt, checkpoint workexecution.Checkpoint,
	observation backingruntimefact.Observation,
) (workexecution.Input, error) {
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || attempt.Executor.Executor != workexecution.Agent ||
		attempt.StartedAt == nil || checkpoint.AttemptID != attempt.ID || checkpoint.Sequence == 0 || checkpoint.Sequence%2 != 0 ||
		checkpoint.Evidence.Validate() != nil || checkpoint.Evidence.ID != attempt.Input.ID ||
		checkpoint.RecordedAt.Before(*attempt.StartedAt) || backingruntimefact.ValidateObservation(observation) != nil ||
		observation.ObservedAt.Before(*attempt.StartedAt) {
		return workexecution.Input{}, invalid("backing creation completion has no matching final runtime observation")
	}
	stageID, err := StageID(attempt.WorkItemID, VerifyRuntime)
	if err != nil || stageID != checkpoint.StageID {
		return workexecution.Input{}, invalid("backing creation completion does not follow final runtime verification")
	}
	attempt.FinishedAt, attempt.Result, attempt.Outcome = nil, nil, ""
	raw, err := json.Marshal(struct {
		Attempt     workexecution.Attempt          `json:"attempt"`
		Checkpoint  workexecution.Checkpoint       `json:"checkpoint"`
		Observation backingruntimefact.Observation `json:"observation"`
	}{attempt, checkpoint, observation})
	if err != nil {
		return workexecution.Input{}, invalid("backing creation completion cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(canonical)
	return workexecution.Input{ID: attempt.Input.ID, SHA256: hex.EncodeToString(digest[:])}, nil
}
