package attachexecution

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type RecoveryInput struct {
	Attempt  workexecution.Attempt
	Source   RemovalRecovery
	Consumer *agentpb.AttachConsumerTransition
}

func (input RecoveryInput) RequiresConsumer() bool {
	authority, checkpoint := input.Source.Failure.Authority, input.Source.Failure.Checkpoint
	return authority.HasConsumer && checkpoint != nil && uint32(checkpoint.Sequence/2) == authority.ConsumerOrdinal
}

func (input RecoveryInput) Validate() error {
	if err := input.Source.ValidateFor(input.Attempt); err != nil {
		return err
	}
	if input.Consumer != nil {
		if !input.RequiresConsumer() {
			return invalid("Attach inspection contains an unrelated consumer transition")
		}
		return validateConsumerAuthority(context.Background(), input.Source.Failure.Authority, input.Consumer)
	}
	return nil
}

func EncodeRecoveryAssignment(input RecoveryInput) (*agentpb.AttachRecoveryAssignment, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	if input.RequiresConsumer() && input.Consumer == nil {
		return nil, invalid("consumer recovery assignment requires its captured transition")
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	source, err := EncodeRemovalRecovery(input.Source)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachRecoveryAssignment{Attempt: attempt, Recovery: source, Consumer: proto.CloneOf(input.Consumer)}, nil
}

func DecodeRecoveryAssignment(message *agentpb.AttachRecoveryAssignment, identity workexecution.Identity) (RecoveryInput, error) {
	if message == nil || executionplan.RejectUnknown(message) != nil {
		return RecoveryInput{}, invalid("Attach recovery assignment is invalid")
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return RecoveryInput{}, err
	}
	source, err := DecodeRemovalRecovery(message.Recovery)
	if err != nil {
		return RecoveryInput{}, err
	}
	input := RecoveryInput{Attempt: attempt, Source: source, Consumer: proto.CloneOf(message.Consumer)}
	if input.RequiresConsumer() && input.Consumer == nil {
		return RecoveryInput{}, invalid("consumer recovery assignment has no captured transition")
	}
	return input, input.Validate()
}

func ClearRecoveryInput(input *RecoveryInput) {
	if input == nil {
		return
	}
	clearRecoveryConsumer(input.Consumer)
	input.Consumer = nil
}

func ClearRecoveryAssignment(message *agentpb.AttachRecoveryAssignment) {
	if message == nil {
		return
	}
	clearRecoveryConsumer(message.Consumer)
	message.Consumer = nil
}

func clearRecoveryConsumer(consumer *agentpb.AttachConsumerTransition) {
	if consumer == nil {
		return
	}
	if consumer.Baseline != nil {
		clear(consumer.Baseline.CanonicalYaml)
	}
	if consumer.Candidate != nil {
		clear(consumer.Candidate.CanonicalYaml)
	}
}
