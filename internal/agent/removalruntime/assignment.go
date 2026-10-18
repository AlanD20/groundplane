// Package removalruntime executes the captured restoration of failed Service
// removals. It cannot select desired state or replay forward removal work.
package removalruntime

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Input struct {
	attempt       workexecution.Attempt
	procedure     *agentpb.ServiceLifecycleWorkInput
	capture       removalexecution.RecoveryCapture
	configuration *agentpb.WorkConfigurationReference
}

func DecodeAssignment(message *agentpb.ServiceRemovalRecoveryAssignment, authenticated workexecution.Identity) (Input, error) {
	if message == nil || proto.Size(message) > executionplan.MaximumPlanBytes || executionplan.RejectUnknown(message) != nil ||
		executionplan.ValidateServiceLifecycleWorkInput(message.Removal) != nil {
		return Input{}, invalidInput()
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, authenticated)
	if err != nil {
		return Input{}, err
	}
	if attempt.Direction != workexecution.Recovery ||
		message.Removal.CapturedSource.InputId != attempt.Input.ID ||
		hex.EncodeToString(message.Removal.CapturedSource.Sha256) != attempt.Input.SHA256 {
		return Input{}, invalidInput()
	}
	capture, err := removalexecution.DecodeCapture(message.Removal, message.CapturedFailure)
	if err != nil {
		return Input{}, err
	}
	if err := capture.ValidateFor(attempt, message.Removal); err != nil {
		return Input{}, err
	}
	if reference := message.Configuration; reference != nil {
		if ids.Validate(ids.KindConfig, reference.ConfigurationId) != nil ||
			reference.EnvironmentId != message.Removal.EnvironmentId || reference.Generation == 0 || len(reference.Sha256) != sha256.Size {
			return Input{}, invalidInput()
		}
	}
	return Input{attempt: attempt, procedure: proto.CloneOf(message.Removal), capture: capture,
		configuration: proto.CloneOf(message.Configuration)}, nil
}

func (input Input) WithStart(ack *agentpb.WorkStartAck) (Input, bool, error) {
	if input.procedure == nil || input.capture.ValidateFor(input.attempt, input.procedure) != nil {
		return Input{}, false, invalidInput()
	}
	started, reconcile, err := workexecution.DecodeAgentStartAck(ack, input.attempt)
	if err != nil {
		return Input{}, false, err
	}
	input.attempt.StartedAt = &started
	return input, reconcile, nil
}

func invalidInput() error {
	return errs.New(errs.KindValidationFailed, "Service removal recovery message differs from its captured host assignment")
}
