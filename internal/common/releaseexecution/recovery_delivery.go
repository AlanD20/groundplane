package releaseexecution

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func EncodeRecoveryAssignment(input RecoveryInput) (*agentpb.ReleaseRecoveryAssignment, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Input.Attempt)
	if err != nil {
		return nil, err
	}
	forward, err := canonicalCheckpoint(input.Forward)
	if err != nil {
		return nil, err
	}
	failure, _, err := EncodeFailure(input.Forward, input.Failure)
	if err != nil {
		return nil, err
	}
	return &agentpb.ReleaseRecoveryAssignment{Work: &agentpb.ReleaseWorkAssignment{Attempt: attempt, Release: proto.CloneOf(input.Input.Release)},
		ForwardAttempt: forward, Failure: failure}, nil
}

func DecodeRecoveryAssignment(message *agentpb.ReleaseRecoveryAssignment, identity workexecution.Identity) (RecoveryInput, error) {
	if message == nil || executionplan.RejectUnknown(message) != nil || message.Work == nil ||
		len(message.ForwardAttempt) == 0 || len(message.ForwardAttempt) > MaximumCheckpointBytes ||
		len(message.Failure) == 0 || len(message.Failure) > MaximumCheckpointBytes {
		return RecoveryInput{}, invalidProgress()
	}
	attempt, release, err := workexecution.DecodeReleaseDelivery(message.Work, identity)
	if err != nil {
		return RecoveryInput{}, err
	}
	if attempt.Direction != workexecution.Recovery {
		return RecoveryInput{}, invalidProgress()
	}
	forward, err := jcs.Decode[workexecution.Attempt](message.ForwardAttempt)
	if err != nil || forward.Validate() != nil || forward.Result == nil {
		return RecoveryInput{}, invalidProgress()
	}
	canonical, err := canonicalCheckpoint(forward)
	if err != nil || !bytes.Equal(canonical, message.ForwardAttempt) {
		return RecoveryInput{}, invalidProgress()
	}
	failure, err := DecodeFailure(forward, message.Failure, *forward.Result)
	if err != nil {
		return RecoveryInput{}, err
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(attempt, release)
	if err != nil {
		return RecoveryInput{}, err
	}
	input := RecoveryInput{Input: Input{Attempt: attempt, Release: release, Artifacts: artifacts}, Forward: forward, Failure: failure}
	if err := input.Validate(); err != nil {
		return RecoveryInput{}, err
	}
	return input, nil
}
