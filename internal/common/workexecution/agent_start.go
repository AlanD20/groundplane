package workexecution

import (
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// DecodeAgentStartAck binds permission to the exact requested attempt. The
// reconcile result must select the operation's inspection path, never its first
// effect. The caller still enforces deadline/cancellation before every effect.
func DecodeAgentStartAck(ack *agentpb.WorkStartAck, expected Attempt) (time.Time, bool, error) {
	if expected.Validate() != nil || expected.Executor.Executor != Agent || expected.FinishedAt != nil ||
		ack == nil || len(ack.ProtoReflect().GetUnknown()) != 0 || ack.StartedAt == nil ||
		ack.StartedAt.CheckValid() != nil || len(ack.StartedAt.ProtoReflect().GetUnknown()) != 0 || !ack.Reconcile && ack.CheckpointSequence != 0 {
		return time.Time{}, false, invalid("Agent start acknowledgement is invalid")
	}
	assigned, err := DecodeAgentAttempt(ack.Attempt, expected.Executor)
	if err != nil {
		return time.Time{}, false, err
	}
	started := ack.StartedAt.AsTime().UTC()
	if !SameAssignment(assigned, expected) || started.Before(expected.AssignedAt) || !started.Before(expected.Deadline) ||
		expected.StartedAt != nil && (!expected.StartedAt.Equal(started) || !ack.Reconcile) {
		return time.Time{}, false, conflict("Agent start acknowledgement differs from the saved assignment")
	}
	return started, ack.Reconcile, nil
}
