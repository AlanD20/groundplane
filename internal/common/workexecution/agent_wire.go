package workexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EncodeAgentAttempt carries immutable assignment identity only. Start and
// terminal receipts are separate acknowledgements, not inferred from delivery.
func EncodeAgentAttempt(attempt Attempt) (*agentpb.WorkAttemptIdentity, error) {
	if attempt.FinishedAt != nil {
		return nil, invalid("Agent delivery requires a live Agent assignment")
	}
	return EncodeAgentAttemptIdentity(attempt)
}

// EncodeAgentAttemptIdentity describes retained ownership for startup accounting.
// It grants no execution and also accepts an already finished local record.
func EncodeAgentAttemptIdentity(attempt Attempt) (*agentpb.WorkAttemptIdentity, error) {
	if err := attempt.Validate(); err != nil {
		return nil, err
	}
	if attempt.Executor.Executor != Agent {
		return nil, invalid("Agent identity requires an Agent assignment")
	}
	digest, err := hex.DecodeString(attempt.Input.SHA256)
	if err != nil {
		return nil, invalid("Agent assignment input digest is invalid")
	}
	direction := agentpb.WorkDirection_WORK_DIRECTION_FORWARD
	if attempt.Direction == Recovery {
		direction = agentpb.WorkDirection_WORK_DIRECTION_RECOVERY
	}
	return &agentpb.WorkAttemptIdentity{
		TaskId: attempt.TaskID, WorkItemId: attempt.WorkItemID, AttemptId: attempt.ID,
		AttemptNumber: attempt.Number, HostId: attempt.Executor.HostID, AgentId: attempt.Executor.AgentID,
		ExecutorGeneration: attempt.Executor.Generation, Direction: direction,
		Input:      &agentpb.WorkInputReference{InputId: attempt.Input.ID, Sha256: digest},
		AssignedAt: timestamppb.New(attempt.AssignedAt), Deadline: timestamppb.New(attempt.Deadline),
	}, nil
}

// DecodeAgentAttempt requires the independently authenticated executor identity.
// It returns captured assignment data, never permission to start or replay effects.
func DecodeAgentAttempt(value *agentpb.WorkAttemptIdentity, authenticated Identity) (Attempt, error) {
	if authenticated.Validate() != nil || authenticated.Executor != Agent || value == nil ||
		len(value.ProtoReflect().GetUnknown()) != 0 || value.Input == nil ||
		len(value.Input.ProtoReflect().GetUnknown()) != 0 || len(value.Input.Sha256) != 32 ||
		value.AssignedAt == nil || value.Deadline == nil ||
		value.AssignedAt.CheckValid() != nil || value.Deadline.CheckValid() != nil ||
		len(value.AssignedAt.ProtoReflect().GetUnknown()) != 0 || len(value.Deadline.ProtoReflect().GetUnknown()) != 0 {
		return Attempt{}, invalid("Agent assignment identity is incomplete or invalid")
	}
	identity := Identity{Executor: Agent, HostID: value.HostId, AgentID: value.AgentId, Generation: value.ExecutorGeneration}
	if identity != authenticated {
		return Attempt{}, conflict("assignment belongs to a different host or Agent generation")
	}
	var direction Direction
	switch value.Direction {
	case agentpb.WorkDirection_WORK_DIRECTION_FORWARD:
		direction = Forward
	case agentpb.WorkDirection_WORK_DIRECTION_RECOVERY:
		direction = Recovery
	default:
		return Attempt{}, invalid("Agent assignment direction is invalid")
	}
	attempt := Attempt{
		ID: value.AttemptId, TaskID: value.TaskId, WorkItemID: value.WorkItemId, Number: value.AttemptNumber,
		Executor: identity, Direction: direction,
		Input:      Input{ID: value.Input.InputId, SHA256: hex.EncodeToString(value.Input.Sha256)},
		AssignedAt: value.AssignedAt.AsTime().UTC(), Deadline: value.Deadline.AsTime().UTC(),
	}
	if err := attempt.Validate(); err != nil {
		return Attempt{}, err
	}
	return attempt, nil
}
