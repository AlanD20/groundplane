package lifecycleexecution

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Continuation carries proof of earlier stages without changing their original
// timestamps or attributing their effects to a new attempt. A recovered boundary
// is carried only with its terminal recovery receipt and matching observation.
type Continuation struct {
	Source     Recovery               `json:"source"`
	Inspection *RecoveryReport        `json:"inspection,omitempty"`
	Inspector  *workexecution.Attempt `json:"inspector,omitempty"`
	// Restored authorizes starting removal again from the restored predecessor,
	// not skipping stages that were undone. The Controller validates the exact
	// restoration checkpoint before publishing this continuation.
	Restored *workexecution.Attempt `json:"restored,omitempty"`
}

func (continuation Continuation) Completed(input *agentpb.ServiceLifecycleWorkInput) ([]Receipt, error) {
	source := continuation.Source
	if err := source.Validate(input); err != nil {
		return nil, err
	}
	if restored := continuation.Restored; restored != nil {
		if continuation.Inspection != nil || continuation.Inspector != nil ||
			input.Action != agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_DESTROY || input.Hook != nil ||
			restored.Validate() != nil || restored.Direction != workexecution.Recovery || restored.Outcome != workexecution.Succeeded ||
			restored.FinishedAt == nil || restored.Result == nil || restored.StartedAt == nil ||
			restored.TaskID != source.Forward.TaskID || restored.WorkItemID != source.Forward.WorkItemID ||
			restored.Input != source.Forward.Input || restored.Executor.HostID != source.Forward.Executor.HostID ||
			restored.Number <= source.Forward.Number || restored.AssignedAt.Before(*source.Forward.FinishedAt) {
			return nil, invalid()
		}
		return nil, nil
	}
	var completed []Receipt
	if source.Progress != nil {
		completed = append(completed, source.Progress.Completed...)
	}
	if source.Progress == nil || source.Progress.BeganAt == nil {
		if continuation.Inspection != nil || continuation.Inspector != nil {
			return nil, invalid()
		}
		return completed, nil
	}
	if continuation.Inspection == nil || continuation.Inspector == nil {
		return nil, invalid()
	}
	attempt, report := *continuation.Inspector, *continuation.Inspection
	if attempt.FinishedAt == nil || attempt.Result == nil || attempt.Outcome != workexecution.Succeeded ||
		!report.BoundaryCompleted() || report.ObservedAt.After(*attempt.FinishedAt) {
		return nil, invalid()
	}
	_, evidence, err := EncodeRecoveryReport(attempt, input, source, report)
	if err != nil || evidence != *attempt.Result {
		return nil, invalid()
	}
	return append(completed, *report.Receipt), nil
}

func (continuation Continuation) ValidateFor(
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
) error {
	if _, err := continuation.Completed(input); err != nil {
		return err
	}
	prior := continuation.Source.Forward
	if continuation.Inspector != nil {
		prior = *continuation.Inspector
	}
	if continuation.Restored != nil {
		prior = *continuation.Restored
	}
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward || attempt.TaskID != prior.TaskID ||
		attempt.WorkItemID != prior.WorkItemID || attempt.Input != prior.Input || attempt.Number <= prior.Number ||
		attempt.Executor.HostID != prior.Executor.HostID || prior.FinishedAt == nil || attempt.AssignedAt.Before(*prior.FinishedAt) {
		return invalid()
	}
	return nil
}

func EncodeContinuation(input *agentpb.ServiceLifecycleWorkInput, continuation Continuation) ([]byte, error) {
	if _, err := continuation.Completed(input); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(continuation)
	if err != nil {
		return nil, invalid()
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil || len(raw) > 2*MaximumRecoveryBytes {
		return nil, invalid()
	}
	return raw, nil
}

func DecodeContinuation(input *agentpb.ServiceLifecycleWorkInput, raw []byte) (Continuation, error) {
	if len(raw) == 0 || len(raw) > 2*MaximumRecoveryBytes {
		return Continuation{}, invalid()
	}
	continuation, err := jcs.Decode[Continuation](raw)
	if err != nil {
		return Continuation{}, invalid()
	}
	expected, err := EncodeContinuation(input, continuation)
	if err != nil || !bytes.Equal(expected, raw) {
		return Continuation{}, invalid()
	}
	return continuation, nil
}
