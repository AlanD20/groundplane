package attachexecution

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ProvisionInput struct {
	Attempt   workexecution.Attempt
	Provision *agentpb.AttachProvisionInput
}

func (input ProvisionInput) Validate() error {
	_, err := input.Authority()
	return err
}

func (input ProvisionInput) Authority() (ProvisionAuthority, error) {
	authority, err := CaptureProvisionAuthority(input.Provision)
	if err != nil {
		return ProvisionAuthority{}, err
	}
	if err := authority.Validate(input.Attempt); err != nil {
		return ProvisionAuthority{}, err
	}
	return authority, nil
}

func DecodeProvisionAssignment(message *agentpb.AttachProvisionAssignment, identity workexecution.Identity) (ProvisionInput, error) {
	if message == nil || executionplan.RejectUnknown(message) != nil {
		return ProvisionInput{}, invalid("Attach provisioning assignment is malformed")
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return ProvisionInput{}, err
	}
	input := ProvisionInput{Attempt: attempt, Provision: message.Provision}
	if _, err := input.Authority(); err != nil {
		return ProvisionInput{}, err
	}
	input.Provision = proto.CloneOf(input.Provision)
	return input, nil
}

func EncodeProvisionAssignment(input ProvisionInput) (*agentpb.AttachProvisionAssignment, error) {
	if _, err := input.Authority(); err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachProvisionAssignment{Attempt: attempt, Provision: proto.CloneOf(input.Provision)}, nil
}
