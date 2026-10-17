package attachplan

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// DetachAssignment resolves the admitted source and its exact encrypted hook
// capture into transient Agent input. It neither rereads desired state nor
// persists the plaintext credential/hook delivery envelope.
func (planner *RemovalPlanner) DetachAssignment(ctx context.Context, captured attachremoval.DetachInput,
	capture attachremoval.WorkCapture, attempt workexecution.Attempt,
) (*agentpb.AttachRemovalAssignment, error) {
	if err := captured.Validate(ctx); err != nil {
		return nil, err
	}
	if attempt.Validate() != nil || attempt.Direction != workexecution.Forward ||
		capture.Validate() != nil || attempt.TaskID != captured.TaskID || attempt.WorkItemID != captured.WorkItemID ||
		capture.TaskID != attempt.TaskID || capture.WorkItemID != attempt.WorkItemID || capture.Action != attempt.Input ||
		capture.BackingProjectID != captured.Snapshot.Source.Attach.BackingProjectID {
		return nil, errs.New(errs.KindStateConflict, "Detach delivery has no matching captured work and hook ownership")
	}
	timestamp, err := ids.Timestamp(ids.KindWorkItem, attempt.WorkItemID)
	if err != nil {
		return nil, err
	}
	operationID := ids.DeriveAt(ids.KindOperation, timestamp, attempt.WorkItemID, "attach-removal-input")
	digest, err := attachremoval.Digest(captured.Snapshot.Source)
	if err != nil || digest != capture.FrozenSHA256 || capture.OperationID != operationID {
		return nil, errs.New(errs.KindStateConflict, "Detach delivery source differs from the admitted encrypted capture")
	}
	if runtime := captured.Snapshot.Runtime; runtime != nil && runtime.Source.Executor.HostID != attempt.Executor.HostID {
		return nil, errs.New(errs.KindStateConflict, "Detach delivery is assigned to a different host than its consumer runtime")
	}
	consumer, err := captured.ConsumerTransition(ctx)
	if err != nil {
		return nil, err
	}
	if consumer != nil {
		defer func() {
			clear(consumer.Baseline.CanonicalYaml)
			clear(consumer.Candidate.CanonicalYaml)
		}()
	}
	source := captured.Snapshot.Source
	capture.ApplyTo(&source)
	raw, err := attachremoval.EncodeWorkCapture(capture)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(raw)
	clear(raw)
	input, err := planner.Build(ctx, source, &agentpb.WorkInputReference{
		InputId: ids.DeriveAt(ids.KindPlan, timestamp, attempt.WorkItemID, "attach-removal-source"), Sha256: hash[:]}, consumer)
	if err != nil {
		return nil, err
	}
	defer attachexecution.ClearRemoval(input)
	return attachexecution.EncodeRemovalAssignment(attachexecution.RemovalInput{Attempt: attempt, Removal: input})
}
