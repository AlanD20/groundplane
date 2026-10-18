package scriptruntime

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// DecodeAssignment takes ownership of the delivery's transient values. The
// caller must clear the returned invocation when its attempt leaves the worker.
// A decoded assignment is not permission to execute: Start must succeed first.
func DecodeAssignment(delivery *agentpb.ScriptWorkAssignment, identity workexecution.Identity) (Invocation, error) {
	invalid := func() (Invocation, error) {
		workexecution.ClearScriptAssignment(delivery)
		return Invocation{}, errs.New(errs.KindValidationFailed, "agent: Script assignment is invalid")
	}
	if delivery == nil || executionplan.RejectUnknown(delivery) != nil ||
		ids.Validate(ids.KindOperation, delivery.OperationId) != nil || delivery.Run == nil ||
		delivery.Snapshot == nil || delivery.Projection == nil || delivery.Body == nil || delivery.Body.Metadata == nil {
		return invalid()
	}
	attempt, err := workexecution.DecodeAgentAttempt(delivery.Attempt, identity)
	if err != nil || attempt.Direction != workexecution.Forward {
		return invalid()
	}
	checkpoint, err := executionplan.ValidateScriptExecutionCheckpoint(delivery.Checkpoint)
	if err != nil || checkpoint.ScriptExecutionId != delivery.Run.ScriptExecutionId ||
		delivery.Snapshot.ScriptExecutionId != delivery.Run.ScriptExecutionId ||
		delivery.Body.Metadata.ScriptExecutionId != delivery.Run.ScriptExecutionId ||
		delivery.Snapshot.SnapshotId != delivery.Run.RunnerSnapshotId ||
		delivery.Projection.SnapshotId != delivery.Run.RunnerSnapshotId {
		return invalid()
	}
	owned := proto.CloneOf(delivery)
	workexecution.ClearScriptAssignment(delivery)
	return Invocation{Attempt: attempt, Input: attempt.Input, OperationID: owned.OperationId,
		Run: owned.Run, Snapshot: owned.Snapshot, Projection: owned.Projection,
		Body: owned.Body, Entries: owned.Entries, Checkpoint: checkpoint}, nil
}

func (invocation *Invocation) Clear() {
	if invocation == nil {
		return
	}
	if invocation.Body != nil {
		clear(invocation.Body.Body)
		invocation.Body.Body = nil
	}
	for _, entry := range invocation.Entries {
		if entry != nil {
			clear(entry.Value)
			entry.Value = nil
		}
	}
}
