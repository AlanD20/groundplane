package lifecycleexecution

import (
	"bytes"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const MaximumRecoveryBytes = MaximumProgressBytes + 8192

// Recovery preserves the failed execution and its exact acknowledged boundary.
// It grants inspection of that captured runtime, not replay of a hook or access
// to newer desired state. Cleanup cannot replace the original failure result.
type Recovery struct {
	Forward  workexecution.Attempt `json:"forward"`
	Failure  Failure               `json:"failure"`
	Evidence workexecution.Input   `json:"evidence"`
	Progress *Progress             `json:"progress,omitempty"`
}

func (source Recovery) Validate(input *agentpb.ServiceLifecycleWorkInput) error {
	stages, err := Stages(input)
	if err != nil || input.CapturedSource == nil || input.CapturedSource.InputId != source.Forward.Input.ID ||
		hex.EncodeToString(input.CapturedSource.Sha256) != source.Forward.Input.SHA256 {
		return invalid()
	}
	return source.ValidateStages(stages)
}

func (source Recovery) ValidateStages(stages []Stage) error {
	forward := source.Forward
	if forward.Validate() != nil || forward.Direction != workexecution.Forward ||
		forward.FinishedAt == nil || forward.Result == nil || forward.Outcome == workexecution.Succeeded ||
		*forward.Result != source.Evidence || !source.Failure.RecordedAt.Equal(*forward.FinishedAt) {
		return invalid()
	}
	_, evidence, err := EncodeFailureStages(forward, stages, source.Failure)
	if err != nil || evidence != source.Evidence {
		return invalid()
	}
	if source.Failure.Checkpoint == nil {
		if source.Progress != nil {
			return invalid()
		}
		return nil
	}
	if forward.StartedAt == nil || source.Progress == nil {
		return invalid()
	}
	checkpoint, err := source.Progress.CheckpointStages(forward, stages)
	if err != nil || checkpoint != *source.Failure.Checkpoint {
		return invalid()
	}
	return nil
}

func (source Recovery) ValidateFor(attempt workexecution.Attempt, input *agentpb.ServiceLifecycleWorkInput) error {
	if err := source.Validate(input); err != nil {
		return err
	}
	stages, err := Stages(input)
	if err != nil {
		return err
	}
	return source.ValidateForStages(attempt, stages)
}

func (source Recovery) ValidateForStages(attempt workexecution.Attempt, stages []Stage) error {
	if source.ValidateStages(stages) != nil || attempt.Validate() != nil || attempt.Direction != workexecution.Recovery ||
		attempt.TaskID != source.Forward.TaskID || attempt.WorkItemID != source.Forward.WorkItemID ||
		attempt.Input != source.Forward.Input || attempt.Number <= source.Forward.Number ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != source.Forward.Executor.HostID {
		return invalid()
	}
	return nil
}

func EncodeRecovery(input *agentpb.ServiceLifecycleWorkInput, source Recovery) ([]byte, error) {
	if err := source.Validate(input); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(source)
	if err != nil {
		return nil, invalid()
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(canonical) > MaximumRecoveryBytes {
		return nil, invalid()
	}
	return canonical, nil
}

func DecodeRecovery(input *agentpb.ServiceLifecycleWorkInput, raw []byte) (Recovery, error) {
	if len(raw) == 0 || len(raw) > MaximumRecoveryBytes {
		return Recovery{}, invalid()
	}
	source, err := jcs.Decode[Recovery](raw)
	if err != nil {
		return Recovery{}, invalid()
	}
	expected, err := EncodeRecovery(input, source)
	if err != nil || !bytes.Equal(expected, raw) {
		return Recovery{}, invalid()
	}
	return source, nil
}
