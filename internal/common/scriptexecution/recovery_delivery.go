package scriptexecution

import (
	"bytes"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func EncodeRecoveryAssignment(input RecoveryInput) (*agentpb.ScriptRecoveryAssignment, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	forward, err := encodeForwardAttempt(input.Forward)
	if err != nil {
		return nil, err
	}
	return &agentpb.ScriptRecoveryAssignment{Attempt: attempt, ForwardAttempt: forward,
		OperationId: input.OperationID, ExecutionId: input.ExecutionID,
		Projection: proto.CloneOf(input.Projection), Artifacts: proto.CloneOf(input.Artifacts)}, nil
}

func DecodeRecoveryAssignment(
	message *agentpb.ScriptRecoveryAssignment,
	identity workexecution.Identity,
) (RecoveryInput, error) {
	if message == nil || executionplan.RejectUnknown(message) != nil || len(message.ForwardAttempt) == 0 ||
		len(message.ForwardAttempt) > 16<<10 {
		return RecoveryInput{}, errs.New(errs.KindValidationFailed, "Script recovery assignment is invalid")
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return RecoveryInput{}, err
	}
	forward, err := jcs.Decode[workexecution.Attempt](message.ForwardAttempt)
	if err != nil {
		return RecoveryInput{}, err
	}
	canonical, err := encodeForwardAttempt(forward)
	if err != nil {
		return RecoveryInput{}, err
	}
	if !bytes.Equal(canonical, message.ForwardAttempt) {
		return RecoveryInput{}, errs.New(errs.KindValidationFailed, "Script recovery forward attempt is not canonical")
	}
	input := RecoveryInput{Attempt: attempt, Forward: forward, OperationID: message.OperationId,
		ExecutionID: message.ExecutionId, Projection: proto.CloneOf(message.Projection), Artifacts: proto.CloneOf(message.Artifacts)}
	if err := input.Validate(); err != nil {
		return RecoveryInput{}, err
	}
	return input, nil
}

func encodeForwardAttempt(attempt workexecution.Attempt) ([]byte, error) {
	if err := attempt.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(attempt)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	if len(raw) > 16<<10 {
		return nil, errs.New(errs.KindValidationFailed, "Script recovery forward attempt exceeds its size limit")
	}
	return raw, nil
}
