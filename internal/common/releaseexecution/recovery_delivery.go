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
	var failure, group []byte
	if input.Group == nil {
		failure, _, err = EncodeFailure(input.Forward, input.Failure)
	} else {
		group, err = canonicalCheckpoint(*input.Group)
	}
	if err != nil {
		return nil, err
	}
	return &agentpb.ReleaseRecoveryAssignment{Work: &agentpb.ReleaseWorkAssignment{Attempt: attempt, Release: proto.CloneOf(input.Input.Release)},
		ForwardAttempt: forward, Failure: failure, GroupRestoration: group}, nil
}

func DecodeRecoveryAssignment(message *agentpb.ReleaseRecoveryAssignment, identity workexecution.Identity) (RecoveryInput, error) {
	if message == nil || executionplan.RejectUnknown(message) != nil || message.Work == nil ||
		len(message.ForwardAttempt) == 0 || len(message.ForwardAttempt) > MaximumCheckpointBytes ||
		(len(message.Failure) == 0) == (len(message.GroupRestoration) == 0) ||
		len(message.Failure) > MaximumCheckpointBytes || len(message.GroupRestoration) > MaximumCheckpointBytes {
		return RecoveryInput{}, invalidProgress()
	}
	attempt, release, err := workexecution.DecodeReleaseDelivery(message.Work, identity)
	if err != nil {
		return RecoveryInput{}, err
	}
	forward, err := jcs.Decode[workexecution.Attempt](message.ForwardAttempt)
	if err != nil || forward.Validate() != nil || forward.Result == nil {
		return RecoveryInput{}, invalidProgress()
	}
	canonical, err := canonicalCheckpoint(forward)
	if err != nil || !bytes.Equal(canonical, message.ForwardAttempt) {
		return RecoveryInput{}, invalidProgress()
	}
	var failure Failure
	var group *GroupRestorationAuthority
	if len(message.Failure) != 0 {
		failure, err = DecodeFailure(forward, message.Failure, *forward.Result)
	} else {
		var decoded GroupRestorationAuthority
		decoded, err = jcs.Decode[GroupRestorationAuthority](message.GroupRestoration)
		if err == nil {
			canonical, encodeErr := canonicalCheckpoint(decoded)
			if encodeErr != nil || !bytes.Equal(canonical, message.GroupRestoration) {
				return RecoveryInput{}, invalidProgress()
			}
			group = &decoded
		}
	}
	if err != nil {
		return RecoveryInput{}, err
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(attempt, release)
	if err != nil {
		return RecoveryInput{}, err
	}
	input := RecoveryInput{Input: Input{Attempt: attempt, Release: release, Artifacts: artifacts}, Forward: forward, Failure: failure, Group: group}
	if err := input.Validate(); err != nil {
		return RecoveryInput{}, err
	}
	return input, nil
}
