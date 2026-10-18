package attachexecution

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ProvisionRecoveryInput contains no credentials or hook script: inspection
// cannot provision, grant privileges or replay a Custom hook.
type ProvisionRecoveryInput struct {
	Attempt  workexecution.Attempt
	Source   ProvisionRecovery
	Consumer *agentpb.AttachConsumerTransition
}

func (input ProvisionRecoveryInput) Validate() error {
	if err := input.Source.ValidateFor(input.Attempt); err != nil {
		return err
	}
	if input.Consumer != nil {
		if !input.RequiresConsumer() {
			return invalid("Attach inspection contains an unrelated consumer transition")
		}
		return validateProvisionConsumerAuthority(context.Background(), input.Source.Failure.Authority, input.Consumer)
	}
	return nil
}

func (input ProvisionRecoveryInput) RequiresConsumer() bool {
	checkpoint := input.Source.Failure.Checkpoint
	return checkpoint != nil && checkpoint.Sequence%2 != 0 && input.Source.Failure.Authority.IsConsumerCommand(uint32((checkpoint.Sequence-1)/2))
}
