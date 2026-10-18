package backingprovision

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// HookSuccessEvidence is emitted only after the Agent has saved the captured
// exec's zero exit status. It names one attempt/container/command input, carries
// no command output or values and is never permission to run that hook again.
func HookSuccessEvidence(attempt workexecution.Attempt, layout Layout, containerID string) (workexecution.Input, error) {
	if layout.Validate(attempt) != nil || attempt.Direction != workexecution.Forward || attempt.StartedAt == nil || !layout.Hook ||
		len(containerID) != 64 {
		return workexecution.Input{}, invalid("backing creation hook result has no captured execution")
	}
	container, err := hex.DecodeString(containerID)
	if err != nil || hex.EncodeToString(container) != containerID {
		return workexecution.Input{}, invalid("backing creation hook result container identity is invalid")
	}
	stageID, err := StageID(attempt.WorkItemID, AfterStart)
	if err != nil {
		return workexecution.Input{}, err
	}
	raw, err := json.Marshal(struct {
		AttemptID   string              `json:"attempt_id"`
		Input       workexecution.Input `json:"input"`
		StageID     string              `json:"stage_id"`
		ContainerID string              `json:"container_id"`
		ExitCode    int32               `json:"exit_code"`
	}{attempt.ID, attempt.Input, stageID, containerID, 0})
	if err != nil {
		return workexecution.Input{}, invalid("backing creation hook result cannot be encoded")
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return workexecution.Input{}, err
	}
	digest := sha256.Sum256(canonical)
	at, err := ids.Timestamp(ids.KindAttempt, attempt.ID)
	if err != nil {
		return workexecution.Input{}, err
	}
	return workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, at, attempt.ID, "backing-creation/after-start-result"),
		SHA256: hex.EncodeToString(digest[:])}, nil
}
